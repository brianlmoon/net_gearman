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
 * @category  Net
 * @package   Net_Gearman
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 * @version   CVS: $Id$
 * @link      http://pear.php.net/package/Net_Gearman
 * @link      http://www.danga.com/gearman/
 */

require_once 'Net/Gearman/Connection.php';
require_once 'Net/Gearman/Set.php';
require_once 'Net/Gearman/Task.php';

/**
 * A client for submitting jobs to Gearman
 *
 * This class is used by code submitting jobs to the Gearman server. It handles
 * taking tasks and sets of tasks and submitting them to the Gearman server.
 *
 * @category  Net
 * @package   Net_Gearman
 * @author    Joe Stump <joe@joestump.net>
 * @copyright 2007-2008 Digg.com, Inc.
 * @license   http://www.opensource.org/licenses/bsd-license.php New BSD License
 * @version   Release: @package_version@
 * @link      http://www.danga.com/gearman/
 */
class Net_Gearman_Client
{
    /**
     * Our randomly selected connection
     *
     * @var resource $conn An open socket to Gearman
     */
    protected $conn = array();

    /**
     * A list of Gearman servers
     *
     * @var array $servers A list of potential Gearman servers
     */
    protected $servers = array();

    /**
     * A list of connections by server name
     *
     * @var array $serverConnections A list of connections by server name
     */
    protected $serversConnections = array();

    /**
     * The timeout for Gearman connections
     *
     * @var integer $timeout
     */
    protected $timeout = 1000;

    /**
     * Constructor
     *
     * @param array   $servers An array of servers or a single server
     * @param integer $timeout Timeout in microseconds
     *
     * @return void
     * @throws Net_Gearman_Exception
     * @see Net_Gearman_Connection
     */
    public function __construct($servers = null, $timeout = 1000)
    {
        if (is_null($servers)){
            $servers = array("localhost");
        } elseif (!is_array($servers) && strlen($servers)) {
            $servers = array($servers);
        } elseif (is_array($servers) && !count($servers)) {
            throw new Net_Gearman_Exception('Invalid servers specified');
        }

        $this->servers = $servers;
        foreach ($this->servers as $key => $server) {
            $server = trim($server);
            if(empty($server)){
                throw new Net_Gearman_Exception('Invalid servers specified');
            }
            $conn = Net_Gearman_Connection::connect($server, $timeout);
            if (!Net_Gearman_Connection::isConnected($conn)) {
                unset($this->servers[$key]);
                continue;
            }

            $this->conn[] = $conn;
            $this->serverConnections[(int)$conn] = $server;
        }

        $this->timeout = $timeout;
    }

    /**
     * Private function to handle the status communication
     *
     * @param  string $handle The handle returned when the task was created
     * @param  string $server The server name(:port) this task was assigned to.
     *                        If not set, all servers in the server list will be checked
     * @return mixed          An associative array containing information about
     *                        the provided task handle. Returns false if the request failed.
     */
    private function getStatus($handle, $server = null)
    {

        $params = array(
            'handle' => $handle,
        );

        if(!is_null($server)){
            $server_list = array($server);
        } else {
            $server_list = $this->servers;
        }

        foreach($server_list as $s) {
            Net_Gearman_Connection::send($s, 'get_status', $params);

            $read = array($s);
            $write = null;
            $except = null;

            socket_select($read, $write, $except, 10);

            foreach ($read as $socket) {
                $resp = Net_Gearman_Connection::read($socket);

                if (isset($resp['function'], $resp['data'])
                    && ($resp['function'] == 'status_res')
                ) {
                    if($resp["data"]["denominator"] > 0){
                        $resp["data"]["percent_complete"] = round(($resp["data"]["numerator"] / $resp["data"]["denominator"]) * 100, 0);
                    } elseif($resp["data"]["running"]) {
                        $resp["data"]["percent_complete"] = 0;
                    } else {
                        $resp["data"]["percent_complete"] = null;
                    }
                    return $resp['data'];
                }
            }
        }

        return false;
    }

    /**
     * Get a connection to a Gearman server
     *
     * @param string  $uniq The unique id of the job
     *
     * @return resource A connection to a Gearman server
     */
    protected function getConnection($uniq = null)
    {
        $conn = null;

        if(count($this->conn) === 1){
            $conn = current($this->conn);
        } elseif($uniq === null){
            $conn = $this->conn[array_rand($this->conn)];
        } else {
            $server = ord(substr(md5($uniq), -1)) % count($this->conn);
            $conn = $this->conn[$server];
        }

        return $conn;
    }

    /**
     * Fire off a background task with the given arguments
     *
     * @param string  $func Name of job to run
     * @param array   $args Arguments for for the magic method call
     *                      First element is the job args.
     *                      Second element is an optional task type
     * @param integer $type Type of job to run task as
     *
     * @return mixed  For background tasks, the task object is returned.
     *                For foreground tasks, the result is returned.
     *
     * @see Net_Gearman_Task
     */
    public function __call($func, array $args)
    {

        if(!isset($args[0])){
            throw new Net_Gearman_Exception('Invalid job arguments');
        }

        $job_args = $args[0];

        if(isset($args[1])){
            $type = $args[1];
        } else {
            $type = Net_Gearman_Task::JOB_BACKGROUND;
        }

        $task = new Net_Gearman_Task($func, $job_args, null, $type);

        $set = new Net_Gearman_Set();
        $set->addTask($task);

        $this->runSet($set);

        if($type == Net_Gearman_Task::JOB_BACKGROUND ||
           $type == Net_Gearman_Task::JOB_HIGH_BACKGROUND ||
           $type == Net_Gearman_Task::JOB_LOW_BACKGROUND){

            $return = $task;

        } else {

            $return = $task->result;
        }

        return $return;
    }

    /**
     * Submit a task to Gearman
     *
     * @param object $task Task to submit to Gearman
     *
     * @return      void
     * @see         Net_Gearman_Task, Net_Gearman_Client::runSet()
     */
    protected function submitTask(Net_Gearman_Task $task)
    {
        switch ($task->type) {
        case Net_Gearman_Task::JOB_LOW:
            $type = 'submit_job_low';
            break;
        case Net_Gearman_Task::JOB_LOW_BACKGROUND:
            $type = 'submit_job_low_bg';
            break;
        case Net_Gearman_Task::JOB_HIGH_BACKGROUND:
            $type = 'submit_job_high_bg';
            break;
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

        // if we don't have a scalar
        // json encode the data
        if(!is_scalar($task->arg)){
            $arg = json_encode($task->arg);
        } else {
            $arg = $task->arg;
        }

        $params = array(
            'func' => $task->func,
            'uniq' => $task->uniq,
            'arg'  => $arg
        );

        $s = $this->getConnection($task->uniq);
        Net_Gearman_Connection::send($s, $type, $params);

        if (!is_array(Net_Gearman_Connection::$waiting[(int)$s])) {
            Net_Gearman_Connection::$waiting[(int)$s] = array();
        }

        array_push(Net_Gearman_Connection::$waiting[(int)$s], $task);

        $task->server = $this->serverConnections[(int)$s];
    }

    /**
     * Run a set of tasks
     *
     * @param object $set A set of tasks to run
     * @param int    $timeout Time in seconds for the socket timeout. Max is 10 seconds
     *
     * @return void
     * @see Net_Gearman_Set, Net_Gearman_Task
     */
    public function runSet(Net_Gearman_Set $set, $timeout = null)
    {
        $totalTasks = $set->tasksCount;
        $taskKeys   = array_keys($set->tasks);
        $t          = 0;

        if ($timeout !== null){
            $socket_timeout = min(10, (int)$timeout);
        } else {
            $socket_timeout = 10;
        }

        while (!$set->finished()) {

            if ($timeout !== null) {

                if (empty($start)) {

                    $start = microtime(true);

                } else {

                    $now = microtime(true);

                    if ($now - $start >= $timeout) {
                        break;
                    }
                }

            }


            if ($t < $totalTasks) {
                $k = $taskKeys[$t];
                $this->submitTask($set->tasks[$k]);
                if ($set->tasks[$k]->type == Net_Gearman_Task::JOB_BACKGROUND ||
                    $set->tasks[$k]->type == Net_Gearman_Task::JOB_HIGH_BACKGROUND ||
                    $set->tasks[$k]->type == Net_Gearman_Task::JOB_LOW_BACKGROUND) {

                    $set->tasks[$k]->finished = true;
                    $set->tasksCount--;
                }

                $t++;
            }

            $write  = null;
            $except = null;
            $read   = $this->conn;
            socket_select($read, $write, $except, $socket_timeout);
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
     * @param array    $resp  The raw array response
     * @param resource $s     The socket
     * @param object   $tasks The tasks being ran
     *
     * @return void
     * @throws Net_Gearman_Exception
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
            $task->complete(json_decode($resp['data']['result'], true));
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
            $task         = array_shift(Net_Gearman_Connection::$waiting[(int)$s]);
            $task->handle = $resp['data']['handle'];
            if ($task->type == Net_Gearman_Task::JOB_BACKGROUND) {
                $task->finished = true;
            }
            $tasks->handles[$task->handle] = $task->uniq;
            break;
        case 'error':
            throw new Net_Gearman_Exception('An error occurred');
        default:
            throw new Net_Gearman_Exception(
                'Invalid function ' . $resp['function']
            );
        }
    }

    /**
     * Disconnect from Gearman
     *
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
     * @return      void
     */
    public function __destruct()
    {
        $this->disconnect();
    }
}

?>
