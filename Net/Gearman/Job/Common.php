<?php

/**
 * Base job class for all Gearman jobs
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

require_once 'Net/Gearman/Job/Exception.php';

/**
 * Base job class for all Gearman jobs
 *
 * @category    Net
 * @package     Net_Gearman
 * @author      Joe Stump <joe@joestump.net> 
 * @link        http://www.danga.com/gearman/
 * @see         Net_Gearman_Job_Common, Net_Gearman_Worker
 */
abstract class Net_Gearman_Job_Common
{
    /**
     * Gearman job handle
     *
     * @access      protected
     * @var         string      $handle
     */
    protected $handle = ''; 
   
    /**
     * Connection to Gearman
     *
     * @access      protected
     * @var         object      $conn           
     * @see         Net_Gearman_Connection
     */
    protected $conn = null;

    /**
     * Constructor
     *
     * @access      public
     * @param       object      $conn           Connection to communicate with
     * @param       string      $handle         Job ID / handle for this job
     */
    public function __construct($conn, $handle)
    {
        $this->conn = $conn;
        $this->handle = $handle;
    }

    /**
     * Run your job here
     *
     * @access      public
     * @param       object      $arg        Arguments passed from the client
     * @return      void
     * @throws      Net_Gearman_Exception
     */
    abstract public function run($arg);

    /**
     * Update Gearman with your job's status
     *
     * @access      public
     * @param       int         $numerator      (e.g. 1)
     * @param       int         $denominator    (e.g. 100)
     * @return      void
     * @see         Net_Gearman_Connection::send()
     */
    public function status($numerator, $denominator) 
    {
        Net_Gearman_Connection::send($this->conn, 'work_status', array(
            'handle' => $this->handle,
            'numerator' => $numerator,
            'denominator' => $denominator
        ));    
    }

    /**
     * Mark your job as complete with its status
     *
     * Net_Gearman communicates between the client and jobs in JSON. The main
     * benefit of this is that we can send fairly complex data types between
     * different languages. You should always pass an array as the result to
     * this function.
     *
     * @access      public
     * @param       array       $result     Result of your job
     * @return      void
     * @see         Net_Gearman_Connection::send()
     */
    public function complete(array $result)
    {
        Net_Gearman_Connection::send($this->conn, 'work_complete', array(
            'handle' => $this->handle,
            'result' => serialize($result)
        ));
    }

    /**
     * Mark your job as failing
     *
     * If your job fails for some reason (e.g. a query fails) you need to run
     * this function and exit from your run() method. This will tell Gearman
     * (and the client by proxy) that the job has failed.
     *
     * @access      public
     * @return      void
     * @see         Net_Gearman_Connection::send()
     */
    public function fail()
    {
        Net_Gearman_Connection::send($this->conn, 'work_fail', array(
            'handle' => $this->handle
        ));
    }
}

?>
