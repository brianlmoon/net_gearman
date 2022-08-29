# Runnings tests
From the project root:

## Install
1. composer install

## Run the unit tests
1. vendor/bin/phpunit -c phpunit.xml.dist

## Run the functional tests
1. Start up your gearman job server
1. Update the `NET_GEARMAN_TEST_SERVER` constant in `phpunit.xml.functional-dist` (if necessary)
1. vendor/bin/phpunit -c phpunit.xml.functional-dist
