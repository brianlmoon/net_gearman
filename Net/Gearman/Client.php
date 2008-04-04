<?php

/**
 * Interface for Danga's Gearman job scheduling system
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
require_once 'Net/Gearman/Set.php';

/**
 * A client for submitting jobs to Gearman
 *
 * This class is used by code submitting jobs to the Gearman server. It handles
 * taking tasks and sets of tasks and submitting them to the Gearman server.
 *
 * @category    Net
 * @package     Net_Gearman
 * @author      Joe Stump <joe@joestump.net> 
 * @link        http://www.danga.com/gearman/
 */
class Net_Gearman_Client
{
    /**
     * Our randomly selected connection
     *
     * @access      protected
     * @var         resource    $conn       An open socket to Gearman
     */
    protected $conn = array();

    /**
     * A list of Gearman servers
     *
     * @access      protected
     * @var         array       $servers    A list of potential Gearman servers
     */
    protected $servers = array();

    /**
     * The timeout for Gearman connections
     *
     * @access      protected
     * @var         integer     $timeout
     */
    protected $timeout = 1000;

    /**
     * Constructor
     *
     * @access      public
     * @param       array       $servers
     * @param       integer     $timeout        Timeout in microseconds
     * @return      void
     * @see         Net_Gearman_Connection
     */
    public function __construct($servers, $timeout = 1000)
    {
        if (!is_array($servers) && strlen($servers)) {
            $servers = array($servers);
        } elseif (is_array($servers) && !count($servers)) {
            throw new Net_Gearman_Exception('Invalid servers specified');
        }

        $this->servers = $servers;
        foreach ($this->servers as $key => $server) {
            $conn = Net_Gearman_Connection::connect($server, $timeout);
            if (!Net_Gearman_Connection::isConnected($conn)) {
                unset($this->servers[$key]);
                continue;
            }

            $this->conn[] = $conn;
        }

        $this->timeout = $timeout;
    }

    /**
     * Get a connection to a Gearman server
     *
     * @access      public
     * @return      resource    A connection to a Gearman server
     */
    protected function getConnection()
    {
        return $this->conn[array_rand($this->conn)];
    }

    /**
     * Fire off a background task with the given arguments
     *
     * @access      public
     * @param       string      $func       Name of job to run
     * @param       array       $args       First key should be args to send 
     * @return      void
     */
    public function __call($func, array $args = array())
    {
        $send = array();
        if (isset($args[0]) && is_array($args[0]) && count($args[0])) {
            $send = $args[0];
        }

        $task = new Net_Gearman_Task($func, $send);
        $task->type = Net_Gearman_Task::JOB_BACKGROUND;

        $set = new Net_Gearman_Set();
        $set->addTask($task);
        $this->runSet($set);
        return $task->handle;
    }

    /**
     * Submit a task to Gearman
     *
     * @access      protected
     * @param       object      $task       Task to submit to Gearman
     * @return      void
     * @see         Net_Gearman_Task, Net_Gearman_Client::runSet()
     */
    protected function submitTask(Net_Gearman_Task $task)
    {
        switch ($task->type) {
        case Net_Gearman_Task::JOB_BACKGROUND:
            $type = 'submit_job_bg';
            break;
        case Net_Gearman_Task::JOB_HIGH:
            $type = 'submit_job_high';
            break;
        default:
            $type = 'submit_job';
            break;
        }

        $params = array(
            'func' => $task->func,
            'uniq' => $task->uniq,
            'arg' => serialize($task->arg)
        );

        $s = $this->getConnection();
        Net_Gearman_Connection::send($s, $type, $params);

        if (!is_array(Net_Gearman_Connection::$waiting[$s])) {
            Net_Gearman_Connection::$waiting[$s] = array();
        }

        array_push(Net_Gearman_Connection::$waiting[$s], $task);
    }

    /**
     * Run a set of tasks
     *
     * @access      public
     * @param       object      $set        A set of tasks to run
     * @return      void
     * @see         Net_Gearman_Set, Net_Gearman_Task
     */
    public function runSet(Net_Gearman_Set $set) 
    {
        $totalTasks = $set->tasksCount;
        $taskKeys = array_keys($set->tasks);
        $t = 0;
        while (!$set->finished()) {
            if ($t < $totalTasks) {
                $k = $taskKeys[$t];
                $this->submitTask($set->tasks[$k]);
                if ($set->tasks[$k]->type == Net_Gearman_Task::JOB_BACKGROUND) {
                    $set->tasks[$k]->finished = true;
                    $set->tasksCount--;
                }

                $t++;
            }

            $write = $except = null;
            $read = $this->conn;
            socket_select($read, $write, $except, 10);
            foreach ($read as $socket) {
                $resp = Net_Gearman_Connection::read($socket);
                if (count($resp)) {
                    $this->handleResponse($resp, $socket, $set);
                }
            }
        }
    }

    /**
     * Handle the response read in 
     *
     * @access      protected
     * @param       array       $resp       The raw array response
     * @param       resource    $s          The socket 
     * @param       object      $tasks      The tasks being ran
     * @return      void
     * @throws      Net_Gearman_Exception
     */
    protected function handleResponse($resp, $s, Net_Gearman_Set $tasks) 
    {
        if (isset($resp['data']['handle']) && 
            $resp['function'] != 'job_created') {
            $task = $tasks->getTask($resp['data']['handle']);
        }

        switch ($resp['function']) {
        case 'work_complete':
            $tasks->tasksCount--;
            $task->complete(unserialize($resp['data']['result']));
            break;
        case 'work_status':
            $n = (int)$resp['data']['numerator'];
            $d = (int)$resp['data']['denominator'];
            $task->status($n, $d);
            break;
        case 'work_fail':
            $tasks->tasksCount--;
            $task->fail();
            break;
        case 'job_created':
            $task = array_shift(Net_Gearman_Connection::$waiting[$s]);
            $task->handle = $resp['data']['handle'];
            if ($task->type == Net_Gearman_Task::JOB_BACKGROUND) {
                $task->finished = true;
            }
            $tasks->handles[$task->handle] = $task->uniq;
            break;
        case 'error':
            throw new Net_Gearman_Exception('An error occurred');
        default:
            throw new Net_Gearman_Exception('Invalid function ' . $resp['function']); 
        }
    }

    /**
     * Disconnect from Gearman
     *
     * @access      public
     * @return      void
     */
    public function disconnect()
    {
        if (!is_array($this->conn) || !count($this->conn)) {
            return;
        }

        foreach ($this->conn as $conn) {
            Net_Gearman_Connection::close($conn);
        }
    }

    /**
     * Destructor
     *
     * @access      public
     * @return      void
     */
    public function __destruct()
    {
        $this->disconnect();
    }
}

?>
