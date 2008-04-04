--TEST--
--FILE--
<?php

require_once 'tests-config.php';
require_once 'Net/Gearman/Client.php';

$sums = array(
    array(1, 2, 5),
    array(12, 34, 100),
    array(120, 1000)
);

$set = new Net_Gearman_Set();
foreach ($sums as $s) {
    $task = new Net_Gearman_Task('Sum', $s);
    $set->addTask($task);
}

$client = new Net_Gearman_Client($servers);
$client->runSet($set);

$superSum = 0;
foreach ($set as $task) {
    $superSum += $task->result->sum;
}

var_dump($superSum);

?>
--EXPECT--
int(1274)
