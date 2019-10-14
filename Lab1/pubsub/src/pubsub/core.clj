(ns pubsub.core
  (:require [clojure.core.async :as async] [clojure.java.io :as io]))

(import '[java.net ServerSocket])

(def not-nil? (complement nil?))
(def not-empty? (complement empty?))
(defn is-hash-set? [topics] (and (not-nil? topics) (instance? java.util.HashSet topics)))

(defn receive [socket]
  "Read from socket"
  (.readLine (io/reader socket))
)

(defn send [socket msg]
  "Send the message string over given socket"
  (let [writer (io/writer socket)]
    (.write writer msg)
    (.flush writer))
)

(definterface IClient
  (getSocket [])
  (getTopics [])
  (addTopic [t])
  (removeTopic [t])
  (hasTopic[t])
  (topicSize[])
)

(deftype Client [socket ^{:volatile-mutable true} topics]
  IClient
  (getSocket [this] socket)
  (getTopics [this] (if (is-hash-set? topics) (.toString topics)))
  (addTopic [this t] (do
                        (set! topics
                          (if (or (nil? topics) ((complement instance?) java.util.HashSet topics))
                            (doto (java.util.HashSet.) (.add t))
                            (doto (java.util.HashSet.) (.addAll topics) (.add t))
                          )
                      )))
  (removeTopic [this t] (set! topics
                          (if (is-hash-set? topics)
                            (doto (java.util.HashSet.) (.addAll topics) (.remove t))
                        )))
  (hasTopic [this t] (if (is-hash-set? topics) (.contains topics t)))
  (topicSize [this] (if (is-hash-set? topics) (.size topics) (int 0)))
)

(def clientList (java.util.ArrayList.))
(def messageList (java.util.HashSet.))

(defn sendTopicMessage [sender topic msg]
  "Send message to all connections who subscribed to the topic"
  (.add messageList msg)
  (doseq [client clientList]
    (if (.hasTopic client topic)
      (do
        (send (.getSocket client) (str (.getPort (.getSocket sender)) "(" topic "): " msg "\n"))
      )
    )
  )
  (str "Message sent to all subscribers of (" topic ")")
)

(defn handler [msg client]
  "Modify the message to send it back"
  (if (not-nil? msg)
    (do
      (def answer "")
      (def tokens (clojure.string/split (clojure.string/lower-case msg) #" "))
      
      (if (>= (count tokens) 2)
        (do
      
          (def cmd (nth tokens 0 ""))
          (def cmd-topic (nth tokens 1))

          (case cmd
            "sub" (do
                    (.addTopic client cmd-topic)
                    (println (.getPort (.getSocket client)) "updated topics:" (.getTopics client))
                    (def answer (str "Subscriptions: " (str (.topicSize client))))
                  )
            "unsub" (do
                    (.removeTopic client cmd-topic)
                    (println (.getPort (.getSocket client)) "updated topics:" (.getTopics client))
                    (def answer (str "Subscriptions: " (str (.topicSize client))))
                    )
            "send" (do
                    (if (>= (count tokens) 3)
                      (do
                        (def answer
                          (sendTopicMessage client
                                            cmd-topic
                                            (subs msg (+ (clojure.string/index-of msg cmd-topic) (count cmd-topic) 1))
                          )
                        )
                      )
                      (def answer "Not enough arguments. Expected 3")
                    )
                  )
            "help" (do
                    (.addTopic client cmd-topic)
                    (println (.getPort (.getSocket client)) " updated topics: " (.getTopics client))
                    (def answer (str "Subscriptions: " (str (.topicSize client))))
                  )
            "default")
        ) ;else if (tokens.size < 2)
        (if (= (nth tokens 0) "help")
          (def answer (str 
            "sub <topic>            - subscribe for additional topic\n"
            "unsub <topic>          - remove topic from your subscriptions\n"
            "send <topic> <message> - send a message to all subscribers of this topic\n"
            "history				- show number of saved messages on the server\n"
            "help                   - display this list\n"
            "quit                   - close connection to the server"))
          (if (= (nth tokens 0) "quit")
            (def answer nil)
            (if (= (nth tokens 0) "history")
              (def answer (str "Number of messages saved on server: " (.size messageList)))
              (def answer "Wrong command.")
            )
          )
        )
      )
      (str answer)
    )
  )
)

(defn display-active-clients []
  (println "Active clients:" (count clientList))
)

(defn client-disconnect [client]
  (.remove clientList client)
  (.close (.getSocket client))
  (display-active-clients)
)

(defn client-thread [client]
  "Method working for a client and running in another thread"
  (.add clientList client)
  (display-active-clients)
  (def alive true)
  
  (while alive
    (let [msg-in (receive (.getSocket client)) msg-out (handler msg-in client)]
      (if (and (not-nil? msg-out) (not-empty? msg-out))
        (send (.getSocket client) (str msg-out "\n"))
        (def alive false) 
      )
    )
  )
  (client-disconnect client)
)

(defn serve-pers [port]
  "Keep receiving connections and providing them with threads"
  (let [running (atom true)]
    (with-open [server-sock (ServerSocket. port)]
      (println "Server started and ready for connections.")
      (while @running
        (let [sock (.accept server-sock)]
          (async/go
          	(def client (Client. sock []))
            (try
              (client-thread client)
            (catch Exception e
              (client-disconnect client))
            )
          )
        )
      )
    )
    running
  )
)

(defn -main []
  "Entry point for starting server"
  (def port 8787)
  (println "Server starting... Port:" port)
  (serve-pers port)
)