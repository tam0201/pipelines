// Code generated by go-swagger; DO NOT EDIT.

package run_model

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"encoding/json"

	strfmt "github.com/go-openapi/strfmt"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/validate"
)

// RunMetricFormat  - FORMAT_UNSPECIFIED: Default value if not present.
//  - RAW: Display value as its raw format.
//  - PERCENTAGE: Display value in percentage format.
// swagger:model RunMetricFormat
type RunMetricFormat string

const (

	// RunMetricFormatFORMATUNSPECIFIED captures enum value "FORMAT_UNSPECIFIED"
	RunMetricFormatFORMATUNSPECIFIED RunMetricFormat = "FORMAT_UNSPECIFIED"

	// RunMetricFormatRAW captures enum value "RAW"
	RunMetricFormatRAW RunMetricFormat = "RAW"

	// RunMetricFormatPERCENTAGE captures enum value "PERCENTAGE"
	RunMetricFormatPERCENTAGE RunMetricFormat = "PERCENTAGE"
)

// for schema
var runMetricFormatEnum []interface{}

func init() {
	var res []RunMetricFormat
	if err := json.Unmarshal([]byte(`["FORMAT_UNSPECIFIED","RAW","PERCENTAGE"]`), &res); err != nil {
		panic(err)
	}
	for _, v := range res {
		runMetricFormatEnum = append(runMetricFormatEnum, v)
	}
}

func (m RunMetricFormat) validateRunMetricFormatEnum(path, location string, value RunMetricFormat) error {
	if err := validate.Enum(path, location, value, runMetricFormatEnum); err != nil {
		return err
	}
	return nil
}

// Validate validates this run metric format
func (m RunMetricFormat) Validate(formats strfmt.Registry) error {
	var res []error

	// value enum
	if err := m.validateRunMetricFormatEnum("", "body", m); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
