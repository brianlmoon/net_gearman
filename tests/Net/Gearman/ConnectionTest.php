<?php

/**
 * Net_Gearman_ConnectionTest
 * @group functional
 */
class Net_Gearman_ConnectionTest extends \PHPUnit\Framework\TestCase
{
    /**
     * When no server is supplied, it should connect to localhost:4730.
     *
     * @return void
     */
    public function testDefaultConnect()
    {

        $connection = new Net_Gearman_Connection();
        $this->assertType('resource', $connection);
        $this->assertEquals('socket', strtolower(get_resource_type($connection->socket)));

        $this->assertTrue($connection->isConnected());

        $connection->close();
    }

    /**
     * 001-echo_req.phpt
     *
     * @return void
     */
    public function testSend()
    {
        $connection = new Net_Gearman_Connection();
        $connection->send('echo_req', array('text' => 'foobar'));

        do {
            $ret = $connection->read();
        } while (is_array($ret) && !count($ret));

        $connection->close();

        $this->assertType('array', $ret);
        $this->assertEquals('echo_res', $ret['function']);
        $this->assertEquals(17, $ret['type']);

        $this->assertType('array', $ret['data']);
        $this->assertEquals('foobar', $ret['data']['text']);
    }
}
