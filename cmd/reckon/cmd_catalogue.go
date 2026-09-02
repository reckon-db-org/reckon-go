package main

import (
	"context"
	"io"

	"github.com/reckon-db-org/reckon-go/cmd/reckon/encode"
)

// reckon catalogue status (gateway-wide) -> CatalogueStatus
func catalogueStatus(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	// Catalogue RPCs are gateway-wide; the bound store_id is ignored.
	s, err := c.Admin("").GetCatalogueStatus(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.CatalogueStatus(s), o.pretty)
}

// reckon catalogue reload (gateway-wide) -> CatalogueReloadResult
func catalogueReload(ctx context.Context, o *opts, _ []string, _ io.Reader, out io.Writer) error {
	c, err := o.dial(ctx)
	if err != nil {
		return err
	}
	defer c.Close()
	rctx, cancel := o.rpcCtx(ctx)
	defer cancel()
	r, err := c.Admin("").ReloadCatalogue(rctx)
	if err != nil {
		return fromGRPC(err)
	}
	return writeJSON(out, encode.CatalogueReload(r), o.pretty)
}
