<?php

/**
 * Gearman worker class
 *
 * PHP version 5.1.0+
 *
 * LICENSE: This source file is subject to the New BSD license that is 
 * available through the world-wide-web at the following URI:
 * http://www.opensource.org/licenses/bsd-license.php. If you did not receive  
 * a copy of the New BSD License and are unable to obtain it through the web, 
 * please send a note to license@php.net so we can mail you a copy immediately.
 *
 * @category    Net
 * @package     Net_Gearman
 * @author      Joe Stump <joe@joestump.net> 
 * @copyright   2007 Digg.com, Inc.
 * @license     http://www.opensource.org/licenses/bsd-license.php 
 * @version     CVS: $Id:$
 * @link        http://pear.php.net/package/Net_Gearman
 * @link        http://www.danga.com/gearman/
 */ 

require_once 'Net/Gearman/Connection.php';
require_once 'Net/Gearman/Job.php';

/**
 * Gearman worker class
 *
 * Run an instance of a worker to listen for jobs. It then manages the running
 * of jobs, etc.
 *
 * <code>
 * <?php
 * 
 * $servers = array(
 *     '127.0.0.1:7003',
 *     '127.0.0.1:7004'
 * );
 * 
 * $abilities = array('HelloWorld', 'Foo', 'Bar');
 * 
 * try {
 *     $worker = new Net_Gearman_Worker($servers);
 *     foreach ($abilities as $ability) {
 *         $worker->addAbility('HelloWorld');
 *     }
 *     $worker->beginWork();
 * } catch (Net_Gearman_Exception $e) {
 *     echo $e->getMessage() . "\n";
 *     exit;
 * } 
 * 
 * ?>
 * </code>
 *
 * @category    Net
 * @package     Net_Gearman
 * @author      Joe Stump <joe@joestump.net> 
 * @link        http://www.danga.com/gearman/
 * @see         Net_Gearman_Job, Net_Gearman_Connection
 */
class Net_Gearman_Worker
{
    /**
     * Pool of connections to Gearman servers
     *
     * @access      private
     * @var         array       $conn
     */
    private $conn = array();

    /**
     * Pool of retry connections
     *
     * @access      private
     * @var         array       $conn
     */
    private $retry_conn = array();

    /**
     * Callbacks registered for this worker
     *
     * @access      private
     * @var         array       $callback
     * @see         Net_Gearman_Worker::JOB_START
     * @see         Net_Gearman_Worker::JOB_COMPLETE
     * @see         Net_Gearman_Worker::JOB_FAIL
     */
    private $callback = array(
        self::JOB_START     => array(),
        self::JOB_COMPLETE  => array(),
        self::JOB_FAIL      => array()
    );

    const JOB_START = 1;
    const JOB_COMPLETE = 2;
    const JOB_FAIL = 3;

    /**
     * Constructor
     *
     * @access      public
     * @param       array       $servers        List of servers to connect to
     * @return      void
     * @see         Net_Gearman_Connection
     */
    public function __construct($servers)
    {
        if (!is_array($servers) && strlen($servers)) {
            $servers = array($servers);
        } elseif (is_array($servers) && !count($servers)) {
            throw new Net_Gearman_Exception('Invalid servers specified');
        }

        foreach ($servers as $s) {
            $conn = Net_Gearman_Connection::connect($s);
            if ($conn === false) {
              $this->retry_conn[$s] = time();
            } else {
              $this->conn[(int)$conn] = $conn;
            }
        }

        if (empty($this->conn)) {
          throw new Net_Gearman_Exception("Couldn't connect to any available servers");
        }
    }

    /**
     * Announce an ability to the job server
     *
     * @access      public
     * @param       string      $ability        Name of functcion/ability
     * @param       int         $timeout        How long to give this job
     */
    public function addAbility($ability, $timeout = null)
    {
        $call = 'can_do';
        $params = array('func' => $ability);
        if (is_int($timeout) && $timeout > 0) {
            $params['timeout'] = $timeout;
            $call = 'can_do_timeout';
        }

        foreach ($this->conn as $conn) {
            Net_Gearman_Connection::send($conn, $call, $params);
        }
    }

    /**
     * Begin working
     *
     * This starts the worker on its journey of actually working. The first
     * argument is a PHP callback to a function that can be used to monitor
     * the worker. If no callback is provided then the worker works until it
     * is killed. The monitor is passed two arguments; whether or not the 
     * worker is idle and when the last job was ran.
     *
     * @access      public
     * @param       callback        $monitor        Function to monitor work
     * @return      void
     */
    public function beginWork($monitor = null)
    {
        if (!is_callable($monitor)) {
            $monitor = array($this, 'stopWork');
        }

        $write = $except = null;
        $working = true;
        $lastJob = time();
        $retryTime = 5;
        while ($working) {
            $sleep = true;
            foreach ($this->conn as $socket) {
                $worked = $this->doWork($socket);
                if ($worked) {
                    $lastJob = time();
                    $sleep = false;
                }
            }
            $idle = false;
            if ($sleep) {
                foreach ($this->conn as $socket) {
                    Net_Gearman_Connection::send($socket, 'pre_sleep');
                }
                $read = $this->conn;
                socket_select($read, $write, $except, 30);
                $errorcode = socket_last_error();
                $errormsg = socket_strerror($errorcode);
                $idle = (count($read) == 0);
            }

            $currentTime = time();
            foreach ($this->retry_conn as $s => $lastTry) {
              if ($lastTry+$retryTime < $currentTime) {
                $conn = Net_Gearman_Connection::connect($s);
                if ($conn !== false) {
                  $this->conn[(int)$conn] = $conn;
                  unset($this->retry_conn[$s]);
                } else {
                  $this->retry_conn[$s] = $currentTime;
                }
              }
            }
            if (call_user_func($monitor, $idle, $lastJob) == true) {
                $working = false;
            }
        }
    }

    /**
     * Listen on the socket for work
     *
     * Sends the 'grab_job' command and then listens for either the 'noop' or
     * the 'no_job' command to come back. If the 'job_assign' comes down the
     * pipe then we run that job. 
     *
     * @access      public
     * @param       resource    $socket
     * @return      boolean     Returns true if work was done, false if not
     */
    private function doWork($socket)
    {
        Net_Gearman_Connection::send($socket, 'grab_job');

        $resp = array('function' => 'noop');
        while (count($resp) && $resp['function'] == 'noop') {
            $resp = Net_Gearman_Connection::blockingRead($socket);
        } 

        if (in_array($resp['function'], array('noop', 'no_job'))) {
            return false;
        }

        if ($resp['function'] != 'job_assign') {
            throw new Net_Gearman_Exception('Holy Cow! What are you doing?!');
        }

        $name = $resp['data']['func'];
        $handle = $resp['data']['handle'];
        $arg = array();
        if (isset($resp['data']['arg']) && 
            mb_strlen($resp['data']['arg'], '8bit')) {
            $arg = unserialize($resp['data']['arg']);
        }

        $job = Net_Gearman_Job::factory($name, $socket, $handle);
        try {
            $this->start($handle, $name, $arg);
            $res = $job->run($arg); 
            if (!is_array($res)) {
                $res = array('result' => $res);
            }

            $job->complete($res);
            $this->complete($handle, $name, $res);
        } catch (Net_Gearman_Job_Exception $e) {
            $job->fail(); 
            $this->fail($handle, $name, $e); 
        }

        // Force the job's destructor to run
        $job = null;

        return true;
    }

    /**
     * Attach a callback
     *
     * @access      public
     * @param       callback        $callback       A valid PHP callback
     * @param       int             $type           Type of callback
     * @return      void
     * @throws      Net_Gearman_Exception
     */
    public function attachCallback($callback, $type = self::JOB_COMPLETE)
    {
        if (!is_callable($callback)) {
            throw new Net_Gearman_Exception('Invalid callback specified');
        }

        $this->callback[$type][] = $callback;
    }

    /**
     * Run the job start callbacks
     *
     * @access      protected
     * @param       string      $handle     The job's Gearman handle
     * @param       string      $job        The name of the job
     * @param       mixed       $arg        The job's argument list
     * @return      void
     */
    protected function start($handle, $job, $args)
    {
        if (!count($this->callback[self::JOB_START])) {
            return; // No callbacks to run
        }

        foreach ($this->callback[self::JOB_START] as $callback) {
            call_user_func($callback, $handle, $job, $args);
        }
    }

    /**
     * Run the complete callbacks
     *
     * @access      protected
     * @param       string      $handle     The job's Gearman handle
     * @param       string      $job        The name of the job
     * @param       array       $result     The job's returned result
     * @return      void
     */
    protected function complete($handle, $job, array $result)
    {
        if (!count($this->callback[self::JOB_COMPLETE])) {
            return; // No callbacks to run
        }

        foreach ($this->callback[self::JOB_COMPLETE] as $callback) {
            call_user_func($callback, $handle, $job, $result);
        }
    }

    /**
     * Run the fail callbacks
     *
     * @access      protected
     * @param       string      $handle     The job's Gearman handle
     * @param       string      $job        The name of the job
     * @param       object      $error      The exception thrown
     * @return      void
     */
    protected function fail($handle, $job, PEAR_Exception $error)
    {
        if (!count($this->callback[self::JOB_FAIL])) {
            return; // No callbacks to run
        }

        foreach ($this->callback[self::JOB_FAIL] as $callback) {
            call_user_func($callback, $handle, $job, $error);
        }
    }

    /**
     * Stop working
     *
     * @access      public
     * @return      void
     */
    public function endWork()
    {
        foreach ($this->conn as $conn) {
            Net_Gearman_Connection::close($conn);
        }
    }

    /**
     * Destructor
     *
     * @access      public
     * @return      void
     * @see         Net_Gearman_Worker::stop()
     */
    public function __destruct()
    {
        $this->endWork();
    }

    /**
     * Should we stop work?
     *
     * @access      public
     * @return      boolean
     */
    public function stopWork()
    {
        return false;
    }
}

?>
