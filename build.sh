cd libs

wget http://downloads.typesafe.com/akka/akka_2.11-2.4.4.zip
unzip akka_2.11-2.4.4.zip

cp akka-2.4.4/lib/scala-library-2.11.8.jar .
cp akka-2.4.4/lib/akka/akka-actor_2.11-2.4.4.jar .
cp akka-2.4.4/lib/akka/config-1.3.0.jar .
cp akka-2.4.4/lib/akka/scala-java8-compat_2.11-0.7.0.jar .

cd ..

ant
