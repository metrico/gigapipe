package dsn

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/jmoiron/sqlx"
)

type StableSqlxDBWrapper struct {
	DB    *sqlx.DB
	mtx   sync.RWMutex
	GetDB func() *sqlx.DB
	Name  string
}

func (s *StableSqlxDBWrapper) Query(query string, args ...any) (*sql.Rows, error) {
	res, err := func() (*sql.Rows, error) {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		res, err := s.DB.Query(query, args...)
		return res, err
	}()
	if err != nil {
		fmt.Println(err)
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.DB.Close()
		s.DB = s.GetDB()
	}
	return res, err
}

func (s *StableSqlxDBWrapper) QueryCtx(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	res, err := func() (*sql.Rows, error) {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		res, err := s.DB.QueryContext(ctx, query, args...)
		return res, err
	}()
	if err != nil {
		fmt.Println(err)
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.DB.Close()
		s.DB = s.GetDB()
	}
	return res, err
}

func (s *StableSqlxDBWrapper) ExecCtx(ctx context.Context, query string, args ...any) error {
	err := func() error {
		s.mtx.RLock()
		defer s.mtx.RUnlock()
		_, err := s.DB.ExecContext(ctx, query, args...)
		return err
	}()
	if err != nil {
		fmt.Println(err)
		s.mtx.Lock()
		defer s.mtx.Unlock()
		s.DB.Close()
		s.DB = s.GetDB()
	}
	return err
}

func (s *StableSqlxDBWrapper) GetName() string {
	return s.Name
}

func (s *StableSqlxDBWrapper) Conn(ctx context.Context) (*sql.Conn, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.DB.Conn(ctx)
}

func (s *StableSqlxDBWrapper) Begin() (*sql.Tx, error) {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	return s.DB.Begin()
}

func (s *StableSqlxDBWrapper) Close() {
	s.mtx.RLock()
	defer s.mtx.RUnlock()
	s.DB.Close()
}
