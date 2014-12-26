package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
)

const maxJobsBuffer int = 50
const maxConnsBuffer int = 50

type Job struct {
	msg string
}

type Result struct {
	msg string
	job *Job
}

type Worker struct {
	id      int
	jobs    <-chan *Job
	results chan<- *Result
}

func (worker *Worker) Run() {
	for {
		select {
		case job := <-worker.jobs:
			result := &Result{
				msg: fmt.Sprintf("Worker %d: %s\n", worker.id, job.msg),
				job: job,
			}
			worker.results <- result
		}
	}
}

type Server struct {
	jobConn map[*Job]net.Conn
	jobs      chan<- *Job
	results   <-chan *Result
	conns     <-chan net.Conn
	workers   []*Worker
}

func (serv *Server) Listen() {
	for _, worker := range serv.workers {
		go worker.Run()
	}

	for {
		select {
		case conn := <-serv.conns:
			reader := bufio.NewReader(conn)
			line, _ := reader.ReadString('\n')
			job := &Job{
				msg: line,
			}
			serv.jobConn[job] = conn
			serv.jobs <- job
		case result := <-serv.results:
			if conn, ok := serv.jobConn[result.job]; ok {
				writer := bufio.NewWriter(conn)
				go func() {
					writer.WriteString(result.msg)
					writer.Flush()
					conn.Close()
					}()
			} else {
				log.Fatal("Could not find connection for job")
			}
		}
	}
}

func NewServer(conns chan net.Conn, workersCount int) *Server {
	jobs := make(chan *Job, maxJobsBuffer)
	results := make(chan *Result, maxJobsBuffer)
	var workers []*Worker

	for i := 0; i < workersCount; i++ {
		worker := &Worker{
			id:      i,
			jobs:    jobs,
			results: results,
		}

		workers = append(workers, worker)
	}

	serv := &Server{
		jobConn: make(map[*Job]net.Conn),
		jobs:      jobs,
		results:   results,
		conns:     conns,
		workers:   workers,
	}

	return serv
}

func main() {
	listener, err := net.Listen("tcp", "localhost:5432")

	if err != nil {
		log.Fatal(err)
	}

	defer listener.Close()
	conns := make(chan net.Conn, maxConnsBuffer)
	serv := NewServer(conns, 5)
	go serv.Listen()

	for {
		conn, err := listener.Accept()

		if err != nil {
			log.Fatal(err)
		}

		conns <- conn
	}
}
