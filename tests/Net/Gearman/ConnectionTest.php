<?php

/**
 * Net_Gearman_ConnectionTest.
 *
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
        $connection = new Net_Gearman_Connection(NET_GEARMAN_TEST_SERVER);
        if (version_compare(PHP_VERSION, '8.0.0') >= 0) {
            // PHP 8+ returns a Socket class instead of a resource now
            $this->assertInstanceOf('Socket', $connection->socket);
        } else {
            $this->assertEquals('socket', strtolower(get_resource_type($connection->socket)));
        }

        $this->assertTrue($connection->isConnected());

        $connection->close();
    }

    /**
     * 001-echo_req.phpt.
     *
     * @return void
     */
    public function testSend()
    {
        $connection = new Net_Gearman_Connection(NET_GEARMAN_TEST_SERVER);
        $this->assertTrue($connection->isConnected());
        $connection->send('echo_req', ['text' => 'foobar']);

        do {
            $ret = $connection->read();
        } while (is_array($ret) && ! count($ret));

        $connection->close();

        $this->assertIsArray($ret);
        $this->assertEquals('echo_res', $ret['function']);
        $this->assertEquals(17, $ret['type']);
        $this->assertIsArray($ret['data']);

        $this->assertEquals('foobar', $ret['data']['text']);
    }
}
