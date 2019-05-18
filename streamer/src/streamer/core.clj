(ns streamer.core
  (:import
   (org.apache.kafka.common.serialization Serde Serdes Serializer)
   (org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder)
   (org.apache.kafka.streams.kstream ValueMapper)
   (org.apache.kafka.streams.kstream Printed)
)
  (:gen-class))

(defn derivative [input-topic]
  (let [builder (StreamsBuilder.)]
    (->
     (.stream builder input-topic) ;; Create the source node of the stream
	 (.print (Printed/toSysOut )))
    builder))


(defn -main [& args]
  (def properties
    {StreamsConfig/APPLICATION_ID_CONFIG, "streamer"
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
     StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))
     StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

  (def config
    (StreamsConfig. properties))

  (def input-topic "kafka-demo")

  (def streams
    (KafkaStreams. 
      (.build (derivative input-topic)) config))

  (.start streams)
  (Thread/sleep (* 60000 10))
)
