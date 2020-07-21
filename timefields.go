package models_utils

import (
	"time"
	"context"
	"github.com/rs/rest-layer/schema"
)

var (
	Now = func(ctx context.Context, value interface{}) interface{} {
		return time.Now().Truncate(time.Microsecond).UTC()
	}

	CreatedField = schema.Field{
		Description: "The time at which the item has been inserted",
		Required:    true,
		ReadOnly:    true,
		OnInit:      Now,
		Sortable:    true,
		Validator:   &schema.Time{},
	}

	UpdatedField = schema.Field{
		Description: "The time at which the item has been last updated",
		Required:    true,
		ReadOnly:    true,
		OnInit:      Now,
		OnUpdate:    Now,
		Sortable:    true,
		Validator:   &schema.Time{},
	}
)