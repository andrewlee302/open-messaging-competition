#!/bin/sh
mvn clean package -U assembly:assembly -Dmaven.test.skip=true
cp target/OpenMessagingDemo/OpenMessagingDemo/lib/*.jar .
if [ -e logs/gc.log ]; then
  mv logs/gc.log "logs/gc-`date +%F-%H-%M-%S`.log"
else
  touch logs/gc.log
fi

java -cp hamcrest-core-1.3.jar:junit-4.12.jar:open-messaging-demo-1.0.jar -Xms2560M -Xmx2560M -verbose:gc -Xloggc:logs/gc.log io.openmessaging.demo.StressProducerTester /Users/andrew/workspace/java/open-messaging-demo/data 40000000
java -cp hamcrest-core-1.3.jar:junit-4.12.jar:open-messaging-demo-1.0.jar -Xms2560M -Xmx2560M -verbose:gc -Xloggc:logs/gc.log io.openmessaging.demo.StressConsumerTester /Users/andrew/workspace/java/open-messaging-demo/data 40000000
#java -cp hamcrest-core-1.3.jar:junit-4.12.jar:open-messaging-demo-1.0.jar -Xms2560M -Xmx2560M -verbose:gc -Xloggc:logs/gc.log io.openmessaging.demo.OrderTester
