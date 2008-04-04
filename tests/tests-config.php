<?php

$path = ini_get('include_path');
ini_set('include_path', realpath('../') . ':' . $path);

// The Gearman servers to use for the various tests
$servers = array('dev01:7003', 'dev01:7004');

?>
