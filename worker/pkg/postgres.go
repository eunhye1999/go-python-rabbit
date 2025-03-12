package pkg

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Postgres struct {
	conn *pgx.Conn
}

// NewPostgres creates a new instance of Postgres with a connection to the database.
func NewPostgres(connString string) (*Postgres, error) {
	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %v", err)
	}
	return &Postgres{conn: conn}, nil
}

// Close closes the database connection.
func (p *Postgres) Close() error {
	return p.conn.Close(context.Background())
}

// ExecuteQuery executes a query and returns the result rows.
func (p *Postgres) ExecuteQuery(ctx context.Context, query string, args ...interface{}) (pgx.Rows, error) {
	rows, err := p.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %v", err)
	}
	return rows, nil
}

// ExecuteQueryRow executes a query that is expected to return a single row and returns the result.
func (p *Postgres) ExecuteQueryRow(ctx context.Context, query string, args ...interface{}) (pgx.Row, error) {
	row := p.conn.QueryRow(ctx, query, args...)
	// QueryRow doesn't return an error directly. It returns a Row, and you check for errors during Scan.
	return row, nil
}
