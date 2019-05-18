(ns streamer.core
  (:import
   (org.apache.kafka.common.serialization Serde Serdes Serializer)
   (org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder)
   (org.apache.kafka.streams KeyValue)
   (org.apache.kafka.streams.kstream ValueMapper)
   (org.apache.kafka.streams.kstream KeyValueMapper)
   (org.apache.kafka.streams.kstream Printed)
)
(:require [cheshire.core :refer :all])
  (:gen-class))

(defn kvalue-mapper [key payload]
 (let [decoded-payload (decode payload)]
  (KeyValue. key {:key key, :id (get decoded-payload "id"), :value (get decoded-payload "value")})
 )
)

(defn derivative [input-topic]
  (let [builder (StreamsBuilder.)]
    (->
     (.stream builder input-topic) ;; Create the source node of the stream
     (.map (reify KeyValueMapper (apply [_ k v] (kvalue-mapper k v))))
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
