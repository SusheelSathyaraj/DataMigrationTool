package database

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

// managing databse connections
type ConnectionPool struct {
	mu          sync.RWMutex
	connections chan *sql.DB
	factory     func() (*sql.DB, error)
	maxSize     int
	currentSize int
	maxIdleTime time.Duration
}

// creating newconnection pools
func NewConnectionPool(maxSize int, factory func() (*sql.DB, error)) *ConnectionPool {
	return &ConnectionPool{
		connections: make(chan *sql.DB, maxSize),
		factory:     factory,
		maxSize:     maxSize,
		maxIdleTime: 5 * time.Minute,
	}
}

// retrieve a connection from the pool
func (p *ConnectionPool) Get() (*sql.DB, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case conn := <-p.connections:
		//testing connections before returning
		if err := conn.Ping(); err != nil {
			p.currentSize--
			return p.createConnection()
		}
		return conn, nil
	default:
		return p.createConnection()
	}
}

// returning connection to the pool
func (p *ConnectionPool) Put(conn *sql.DB) {
	if conn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	select {
	case p.connections <- conn:
		//connection returned to the pool
	default:
		//pool is full, closing connection
		conn.Close()
		p.currentSize--
	}
}

// closing all connections in the pool
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	close(p.connections)

	for conn := range p.connections {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	p.currentSize = 0
	return nil
}

// creating a new database conneciton
func (p *ConnectionPool) createConnection() (*sql.DB, error) {
	if p.currentSize >= p.maxSize {
		return nil, fmt.Errorf("connection pool is full")
	}

	conn, err := p.factory()
	if err != nil {
		return nil, err
	}

	p.currentSize++
	return conn, nil
}

// creating a mysql connection pool
func NewMySQLConnectionPool(user, password, host string, port int, dbname string, maxSize int) *ConnectionPool {
	factory := func() (*sql.DB, error) {
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", user, password, host, port, dbname)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}

		//configuring connection settings
		db.SetMaxOpenConns(maxSize)
		db.SetMaxIdleConns(maxSize / 2)
		db.SetConnMaxLifetime(time.Hour)

		return db, db.Ping()
	}
	return NewConnectionPool(maxSize, factory)
}
