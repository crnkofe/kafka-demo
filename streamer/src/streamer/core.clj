(ns streamer.core
  (:import
   (org.apache.kafka.common.serialization Serde Serdes Serializer)
   (org.apache.kafka.streams StreamsConfig KafkaStreams StreamsBuilder)
   (org.apache.kafka.streams KeyValue)
   (org.apache.kafka.common.serialization Serializer)
   (org.apache.kafka.common.serialization Deserializer)
   (org.apache.kafka.streams.kstream ForeachAction)
   (java.io ByteArrayOutputStream)
   (java.io ByteArrayInputStream)
   (org.apache.kafka.streams.kstream ValueMapper)
   (org.apache.kafka.streams.kstream Produced)
   (org.apache.kafka.streams.kstream Serialized)
   (org.apache.kafka.streams.kstream KeyValueMapper)
   (org.apache.kafka.streams.kstream Printed)
   (org.apache.kafka.streams.kstream TimeWindows)
   (org.apache.kafka.streams.kstream Reducer)
   (org.apache.kafka.streams.kstream SessionWindows)
   (org.apache.kafka.streams.kstream WindowedSerdes)
   (org.apache.kafka.streams.kstream Grouped)
   (org.apache.kafka.streams.kstream Materialized)
   (java.time Duration)
)
(:require [cheshire.core :refer :all])
  (:gen-class))

(deftype JsonSerializer [opts]
  Serializer
  (configure [_ _ _])
  (serialize [_ _ data]
    (when data
      (with-open [baos (ByteArrayOutputStream.)
                  writer (clojure.java.io/writer baos)]
        (generate-stream data writer opts)
        (.toByteArray baos))))
  (close [_]))

(defn json-serializer
  "JSON serializer for Apache Kafka.
  Use for serializing Kafka keys values.
  > Notes: You may pass any of the built-in Chesire options for generate-stream via the opts map, using
   the 1-arity version of this function."
  (^JsonSerializer [] (json-serializer nil))
  (^JsonSerializer [opts]
(JsonSerializer. opts)))

(deftype JsonDeserializer [opts]
  Deserializer
  (configure [_ _ _])
  (deserialize [_ _ data]
    (when data
      ;;possible to move this to store in type somewhere
      ;;the parse-stream function doesn't take its options the same way as generate-stream
      ;;we could call apply on it, but want to leave room for future options without breaking signatures
      (let [{:keys [key-fn array-coerce-fn]} opts]
        (with-open [bais (ByteArrayInputStream. data)
                    reader (clojure.java.io/reader bais)]
          (parse-stream reader key-fn array-coerce-fn)))))
  (close [_]))

(defn json-deserializer
  "JSON deserializer for Apache Kafka.
  Use for deserializing Kafka keys and values.
  > Notes: You may pass any of the built-in Cheshire options to parse-stream via the opts map, using
   the 1-arity version of this function."
  (^JsonDeserializer [] (json-deserializer nil))
  (^JsonDeserializer [opts]
(JsonDeserializer. opts)))



(defn kvalue-mapper-default [key payload]
 (let [decoded-payload (decode payload)]
  (KeyValue. (get decoded-payload "ts") {:ts (get decoded-payload "ts"), :id (get decoded-payload "id"), :value (get decoded-payload "value")})
 ))

;; attach seconds timestamp as key to stream
(defn kvalue-mapper [key payload]
 (let [decoded-payload (decode payload)]
  (KeyValue. (quot (get decoded-payload "ts") 1000) {:ts (get decoded-payload "ts"), :id (get decoded-payload "id"), :value (get decoded-payload "value")})
 ))


(defn kvalue-mapper-seconds [key payload]
 (let [decoded-payload (decode payload)]
  (KeyValue. (quot (get decoded-payload "ts") 1000) {:ts (get decoded-payload "ts"), :id (get decoded-payload "id"), :value (get decoded-payload "value")})
 ))


(defn kvalue-mapper-minutes [key payload]
 (let [decoded-payload (decode payload)]
  (KeyValue. (quot (quot (get decoded-payload "ts") 1000) 60) {:ts (get decoded-payload "ts"), :id (get decoded-payload "id"), :value (get decoded-payload "value")})
 ))


(defn kvalue-mapper-minutes-five [key payload]
 (let [decoded-payload (decode payload)]
  (KeyValue. (quot (quot (quot (get decoded-payload "ts") 1000) 60) 5) {:ts (get decoded-payload "ts"), :id (get decoded-payload "id"), :value (get decoded-payload "value")})
 ))

(defn identity-map [input-topic]
  (let [builder (StreamsBuilder.)]
    (->
     (.stream builder input-topic) ;; Create the source node of the stream
     (.map (reify KeyValueMapper (apply [_ k v] (kvalue-mapper k v))))
	 (.print (Printed/toSysOut )))
    builder))

(defn identity-stream [input-topic]
  (let [builder (StreamsBuilder.)
        window (.advanceBy (TimeWindows/of  60000) 1000)
		jsonSerdes (Serdes/serdeFrom (JsonSerializer. []) (JsonDeserializer. []))
		serializer (Serialized/with (Serdes/String) (Serdes/Long))
		string-serializer (Serialized/with (Serdes/String) (Serdes/Long))]
	(try
	  (->
		  (.stream builder input-topic) ;; Create the source node of the stream
		  (.map (reify KeyValueMapper (apply [_ k v] (kvalue-mapper-minutes k v))))
		  (.to "kafka-demo-count" (Produced/with (Serdes/Long) jsonSerdes)))
      (catch Exception e (println (str "caught exception: " (.toString e))))
      (finally (println "This is our final block")))
    builder))

(defn count-stream [input-topic]
  (let [builder (StreamsBuilder.)
        window (.advanceBy (TimeWindows/of  60000) 1000)
		jsonSerdes (Serdes/serdeFrom (JsonSerializer. []) (JsonDeserializer. []))
		serializer (Serialized/with (Serdes/String) (Serdes/Long))
		string-serializer (Serialized/with (Serdes/String) (Serdes/Long))]
	(try
	  (->
		  (.stream builder input-topic) ;; Create the source node of the stream
		  (.map (reify KeyValueMapper (apply [_ k v] (kvalue-mapper-minutes k v))))
          (.groupByKey (Serialized/with (Serdes/Long) jsonSerdes))
          (.count)
          (.toStream)
		  (.to "kafka-demo-count" (Produced/with (Serdes/Long) jsonSerdes)))
      (catch Exception e (println (str "caught exception: " (.toString e))))
      (finally (println "This is our final block")))
    builder))

(defn count-window-stream [input-topic]
  "Group timestamped values by 5 minutes
  Window them by 1 minute non-overlapping intervals
  and compute counted values in that window
  output that to a different topic
  "
  (let [builder (StreamsBuilder.)
        window (.advanceBy (TimeWindows/of  60000) 1000)
		jsonSerdes (Serdes/serdeFrom (JsonSerializer. []) (JsonDeserializer. []))
        windowSerdes (WindowedSerdes/timeWindowedSerdeFrom Long)
		serializer (Serialized/with (Serdes/String) (Serdes/Long))
		string-serializer (Serialized/with (Serdes/String) (Serdes/Long))]
	(try
	  (->
		  (.stream builder input-topic) ;; Create the source node of the stream
		  (.map (reify KeyValueMapper (apply [_ k v] (kvalue-mapper-minutes-five k v))))
          (.groupByKey (Serialized/with (Serdes/Long) jsonSerdes))
          (.windowedBy (.advanceBy (TimeWindows/of (Duration/ofMinutes 1) ) (Duration/ofSeconds 60)))
          (.count (Materialized/with (Serdes/Long) (Serdes/Long)))
          (.toStream)
		  (.to "kafka-demo-count" (Produced/with windowSerdes (Serdes/Long))))
      (catch Exception e (println (str "caught exception: " (.toString e)))))
    builder))

(defn difference [p n]
  (let [diff (- (get n "value") (get p "value"))]
    ;; (println diff (get p "ts") (get n "ts"))
    ;; (println diff (get p "value") (get n "value"))
    {"ts" (get n "ts"), "id" (get n "id"), "value" (get n "value"), "difference" diff}
  )
  )

(defn derivative-stream [input-topic]
  "Group timestamped values by 1 minute

  and compute counted values in that window
  output that to a different topic
  "
  (let [builder (StreamsBuilder.)
        window (.advanceBy (TimeWindows/of  60000) 1000)
		jsonSerdes (Serdes/serdeFrom (JsonSerializer. []) (JsonDeserializer. []))
        windowSerdes (WindowedSerdes/timeWindowedSerdeFrom Long)
		serializer (Serialized/with (Serdes/String) (Serdes/Long))
		string-serializer (Serialized/with (Serdes/String) (Serdes/Long))]
	(try
	  (->
		  (.stream builder input-topic) ;; Create the source node of the stream
		  (.map (reify KeyValueMapper (apply [_ k v] (kvalue-mapper-minutes k v))))
          (.groupByKey (Serialized/with (Serdes/Long) jsonSerdes))
          (.windowedBy (.advanceBy (TimeWindows/of (Duration/ofSeconds 10) ) (Duration/ofSeconds 10)))
          (.reduce (reify Reducer (apply [_ p n] (difference p n))))
          (.toStream)
		  (.to "kafka-demo-derivative" (Produced/with windowSerdes jsonSerdes)))
      (catch Exception e (println (str "caught exception: " (.toString e)))))
    builder))

(defn -main [& args]
  (def properties
    {StreamsConfig/APPLICATION_ID_CONFIG, "streamer"
     StreamsConfig/BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
     StreamsConfig/DEFAULT_KEY_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/Long)))
     StreamsConfig/DEFAULT_VALUE_SERDE_CLASS_CONFIG, (.getName (.getClass (Serdes/String)))})

  (def config
    (StreamsConfig. properties))

  (def input-topic "kafka-demo")

  (def streams
    (KafkaStreams. 
      (.build (derivative-stream input-topic)) config))

  (.start streams)
  (Thread/sleep (* 60000 10))
)
