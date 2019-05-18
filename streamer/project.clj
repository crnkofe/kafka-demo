(defproject streamer "1.0"
  :description "Kafka demo KStream and KTable processor"
  :url "http://github.com/crnkofe"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/data.json "0.2.6"]
                 [org.apache.kafka/kafka-streams "2.0.0"]
                 [org.apache.kafka/kafka-clients "2.0.0"]
                 [org.apache.kafka/kafka-streams-test-utils "2.0.0"]
                 [org.apache.kafka/kafka-clients "1.1.0" :classifier "test"]
                 [org.apache.kafka/connect-json "2.0.0"]
                 [org.springframework.kafka/spring-kafka "1.2.2.RELEASE"]
                 [org.slf4j/slf4j-log4j12 "1.7.1"]
                 [log4j/log4j "1.2.17" :exclusions [javax.mail/mail
                                                    javax.jms/jms
                                                    com.sun.jmdk/jmxtools
                                                    com.sun.jmx/jmxri]]
                 ]
  :main ^:skip-aot streamer.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
