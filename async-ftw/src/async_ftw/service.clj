(ns async-ftw.service
    (:require [io.pedestal.service.http :as bootstrap]
              [io.pedestal.service.http.route :as route]
              [io.pedestal.service.http.body-params :as body-params]
              [io.pedestal.service.http.route.definition :refer [defroutes]]
              [io.pedestal.service.impl.interceptor :refer [with-pause resume]]
              [io.pedestal.service.interceptor :refer [definterceptor] :as interceptor]
              [io.pedestal.service.log :as log]
              [clojure.core.async :refer [chan >! go]]
              [ring.util.response :as ring-resp]))

(defn log-thread-id [where]
  (let [thread-id (.. Thread currentThread getId)]
    (log/info :msg (str "Current thread is: " thread-id)
              :where where)))

(defn home-page
  [request]
  (log-thread-id "home-page")
  (ring-resp/response "Hello World!"))

(definterceptor async-to-the-max
  "Go Async, logging thread IDs pre-pause and post-resume."
  (interceptor/interceptor
    :name ::async-to-the-max
    :enter (fn [context]
              (log-thread-id "before with-pause")
              (let [c (chan)]
                (go
                  (Thread/sleep 1000)
                  (log-thread-id "Pre-put")
                  (>! c context)
                  (log-thread-id "After-put"))
                c))
    :pause (fn [c] (log-thread-id "pause")
              c)
    :resume (fn [c] (log-thread-id "resume")
               c)))

(defroutes routes
  [[["/" ^:interceptors [async-to-the-max] {:get home-page}]]])

;; You can use this fn or a per-request fn via io.pedestal.service.http.route/url-for
(def url-for (route/url-for-routes routes))

;; Consumed by async-ftw.server/create-server
(def service {:env :prod
              ;; You can bring your own non-default interceptors. Make
              ;; sure you include routing and set it up right for
              ;; dev-mode. If you do, many other keys for configuring
              ;; default interceptors will be ignored.
              ;; :bootstrap/interceptors []
              ::bootstrap/routes routes

              ;; Uncomment next line to enable CORS support, add
              ;; string(s) specifying scheme, host and port for
              ;; allowed source(s):
              ;;
              ;; "http://localhost:8080"
              ;;
              ;;::bootstrap/allowed-origins ["scheme://host:port"]

              ;; Root for resource interceptor that is available by default.
              ::bootstrap/resource-path "/public"

              ;; Either :jetty or :tomcat (see comments in project.clj
              ;; to enable Tomcat)
              ;;::bootstrap/host "localhost"
              ::bootstrap/type :jetty
              ::bootstrap/port 8080})
