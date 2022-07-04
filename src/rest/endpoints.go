package rest

import (
	"fmt"
	"github.com/go-chi/chi/v5"
	"io/ioutil"
	"log"
	"net/http"
)

func InitServer(processor func(topic string, key *string, partition *string, message string) (offset int64, resultPartition int, er error)) {
	router := chi.NewRouter()

	router.Get("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		log.Println("ok, todo: add kafka client healthcheck")
	})

	router.Post("/send-message/to-topic/{topic}", func(w http.ResponseWriter, r *http.Request) {
		wrap(w, r, processor)
	})

	router.Post("/send-message/to-topic/{topic}/key/{key}", func(w http.ResponseWriter, r *http.Request) {
		wrap(w, r, processor)
	})

	router.Post("/send-message/to-topic/{topic}/key/{key}/partition/{partition}", func(w http.ResponseWriter, r *http.Request) {
		wrap(w, r, processor)
	})

	er := http.ListenAndServe(":3000", router)
	if er != nil {
		log.Println("failed to start webserver with error " + er.Error())
	}
}

func wrap(w http.ResponseWriter, r *http.Request, processor func(topic string, key *string, partition *string, message string) (offset int64, resultPartition int, er error)) {
	topic := chi.URLParam(r, "topic")
	if topic == "" {
		http.Error(w, "empty string as topic name is not allowed", 400)
		return
	}
	key := chi.URLParam(r, "key")
	var k *string = nil
	if key != "" {
		k = &key
	}

	partition := chi.URLParam(r, "partition")
	var p *string = nil
	if partition != "" {
		p = &partition
	}
	defer r.Body.Close()

	bbody, er := ioutil.ReadAll(r.Body)
	if er != nil {
		http.Error(w, er.Error(), 500)
		return
	}
	offset, ption, er := processor(topic, k, p, string(bbody))
	if er != nil {
		http.Error(w, er.Error(), 500)
		return
	}
	_, er = w.Write([]byte(fmt.Sprintf("{\"offset\":%d, \"partition\": %d}", offset, ption)))
	if er != nil {
		http.Error(w, er.Error(), 500)
	}
}
