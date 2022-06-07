// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016 Datadog, Inc.

package sql // import "gopkg.in/DataDog/dd-trace-go.v1/contrib/database/sql"

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"gopkg.in/DataDog/dd-trace-go.v1/internal/log"
)

var _ driver.Conn = (*tracedConn)(nil)

type queryType string

const (
	queryTypeConnect  queryType = "Connect"
	queryTypeQuery              = "Query"
	queryTypePing               = "Ping"
	queryTypePrepare            = "Prepare"
	queryTypeExec               = "Exec"
	queryTypeBegin              = "Begin"
	queryTypeClose              = "Close"
	queryTypeCommit             = "Commit"
	queryTypeRollback           = "Rollback"
)

type tracedConn struct {
	driver.Conn
	*traceParams
}

func (tc *tracedConn) BeginTx(ctx context.Context, opts driver.TxOptions) (tx driver.Tx, err error) {
	start := time.Now()
	if connBeginTx, ok := tc.Conn.(driver.ConnBeginTx); ok {
		tx, err = connBeginTx.BeginTx(ctx, opts)
		tc.tryTrace(ctx, queryTypeBegin, "", start, err)
		if err != nil {
			return nil, err
		}
		return &tracedTx{tx, tc.traceParams, ctx}, nil
	}
	tx, err = tc.Conn.Begin()
	tc.tryTrace(ctx, queryTypeBegin, "", start, err)
	if err != nil {
		return nil, err
	}
	return &tracedTx{tx, tc.traceParams, ctx}, nil
}

func (tc *tracedConn) PrepareContext(ctx context.Context, query string) (stmt driver.Stmt, err error) {
	start := time.Now()
	cquery, spanID := tc.injectComments(ctx, query, true)
	if connPrepareCtx, ok := tc.Conn.(driver.ConnPrepareContext); ok {
		stmt, err := connPrepareCtx.PrepareContext(ctx, cquery)
		tc.tryTrace(ctx, queryTypePrepare, cquery, start, err, tracer.WithSpanID(spanID))
		if err != nil {
			return nil, err
		}

		return &tracedStmt{Stmt: stmt, traceParams: tc.traceParams, ctx: ctx, query: cquery}, nil
	}
	stmt, err = tc.Prepare(cquery)
	tc.tryTrace(ctx, queryTypePrepare, cquery, start, err, tracer.WithSpanID(spanID))
	if err != nil {
		return nil, err
	}

	return &tracedStmt{Stmt: stmt, traceParams: tc.traceParams, ctx: ctx, query: cquery}, nil
}

func (tc *tracedConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (r driver.Result, err error) {
	start := time.Now()
	if execContext, ok := tc.Conn.(driver.ExecerContext); ok {
		cquery, spanID := tc.injectComments(ctx, query, false)
		r, err := execContext.ExecContext(ctx, cquery, args)
		tc.tryTrace(ctx, queryTypeExec, query, start, err, tracer.WithSpanID(spanID))
		return r, err
	}
	if execer, ok := tc.Conn.(driver.Execer); ok {
		dargs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		cquery, spanID := tc.injectComments(ctx, query, false)
		r, err = execer.Exec(cquery, dargs)
		tc.tryTrace(ctx, queryTypeExec, query, start, err, tracer.WithSpanID(spanID))
		return r, err
	}
	return nil, driver.ErrSkip
}

// tracedConn has a Ping method in order to implement the pinger interface
func (tc *tracedConn) Ping(ctx context.Context) (err error) {
	start := time.Now()
	if pinger, ok := tc.Conn.(driver.Pinger); ok {
		err = pinger.Ping(ctx)
	}
	tc.tryTrace(ctx, queryTypePing, "", start, err)
	return err
}

func (tc *tracedConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (rows driver.Rows, err error) {
	start := time.Now()
	if queryerContext, ok := tc.Conn.(driver.QueryerContext); ok {
		cquery, spanID := tc.injectComments(ctx, query, false)
		rows, err := queryerContext.QueryContext(ctx, cquery, args)
		tc.tryTrace(ctx, queryTypeQuery, cquery, start, err, tracer.WithSpanID(spanID))
		return rows, err
	}
	if queryer, ok := tc.Conn.(driver.Queryer); ok {
		dargs, err := namedValueToValue(args)
		if err != nil {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		cquery, spanID := tc.injectComments(ctx, query, true)
		rows, err = queryer.Query(cquery, dargs)
		tc.tryTrace(ctx, queryTypeQuery, cquery, start, err, tracer.WithSpanID(spanID))
		return rows, err
	}
	return nil, driver.ErrSkip
}

func (tc *tracedConn) CheckNamedValue(value *driver.NamedValue) error {
	if checker, ok := tc.Conn.(driver.NamedValueChecker); ok {
		return checker.CheckNamedValue(value)
	}
	return driver.ErrSkip
}

var _ driver.SessionResetter = (*tracedConn)(nil)

// ResetSession implements driver.SessionResetter
func (tc *tracedConn) ResetSession(ctx context.Context) error {
	if resetter, ok := tc.Conn.(driver.SessionResetter); ok {
		return resetter.ResetSession(ctx)
	}
	// If driver doesn't implement driver.SessionResetter there's nothing to do
	return nil
}

// traceParams stores all information related to tracing the driver.Conn
type traceParams struct {
	cfg        *config
	driverName string
	meta       map[string]string
}

type contextKey int

const spanTagsKey contextKey = 0 // map[string]string

// WithSpanTags creates a new context containing the given set of tags. They will be added
// to any query created with the returned context.
func WithSpanTags(ctx context.Context, tags map[string]string) context.Context {
	return context.WithValue(ctx, spanTagsKey, tags)
}

// injectComments returns the query with sql comments injected according to the comment injection mode along
// with a span id injected into sql comments. If a span ID is returned, the caller should make sure to use it when creating
// the span following the traced database call.
func (tp *traceParams) injectComments(ctx context.Context, query string, discardTracingTags bool) (cquery string, spanID uint64) {
	// The sql span only gets created after the call to the database because we need to be able to skip spans
	// when a driver returns driver.ErrSkip. In order to work with those constraints, a new span id is generated and
	// used during SQL comment injection and returned for the sql span to be used later when/if the span
	// gets created.
	var spanContext ddtrace.SpanContext
	if span, ok := tracer.SpanFromContext(ctx); ok {
		spanContext = span.Context()
	}
	sqlCommentCarrier := tracer.NewSQLCommentCarrier(query, resolveInjectionMode(tp.cfg.commentInjectionMode, discardTracingTags))
	err := tracer.Inject(spanContext, sqlCommentCarrier)
	if err != nil {
		// This should happen only if the SQLCommentPropagator is not set via the sql comment injection mode.
		log.Warn("contrib/database/sql: failed to inject query comments. Make sure you've set up SQLCommentInjectionMode on the propagator configuration: %v", err)
	}
	return sqlCommentCarrier.Query, sqlCommentCarrier.SpanID
}

func resolveInjectionMode(mode tracer.SQLCommentInjectionMode, discardTracingTags bool) tracer.SQLCommentInjectionMode {
	if discardTracingTags && mode == tracer.FullSQLCommentInjection {
		mode = tracer.ServiceTagsInjection
	}
	return mode
}

// tryTrace will create a span using the given arguments, but will act as a no-op when err is driver.ErrSkip.
func (tp *traceParams) tryTrace(ctx context.Context, qtype queryType, query string, startTime time.Time, err error, spanOpts ...ddtrace.StartSpanOption) {
	if err == driver.ErrSkip {
		// Not a user error: driver is telling sql package that an
		// optional interface method is not implemented. There is
		// nothing to trace here.
		// See: https://github.com/DataDog/dd-trace-go/issues/270
		return
	}
	if _, exists := tracer.SpanFromContext(ctx); tp.cfg.childSpansOnly && !exists {
		return
	}
	name := fmt.Sprintf("%s.query", tp.driverName)
	opts := append(spanOpts,
		tracer.ServiceName(tp.cfg.serviceName),
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.StartTime(startTime),
	)
	if !math.IsNaN(tp.cfg.analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, tp.cfg.analyticsRate))
	}
	span, _ := tracer.StartSpanFromContext(ctx, name, opts...)
	resource := string(qtype)
	if query != "" {
		resource = query
	}
	span.SetTag("sql.query_type", string(qtype))
	span.SetTag(ext.ResourceName, resource)
	for k, v := range tp.meta {
		span.SetTag(k, v)
	}
	if meta, ok := ctx.Value(spanTagsKey).(map[string]string); ok {
		for k, v := range meta {
			span.SetTag(k, v)
		}
	}
	span.Finish(tracer.WithError(err))
}
