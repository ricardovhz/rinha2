package model

import "fmt"

type Transaction struct {
	Date        string `json:"realizada_em"`
	Value       int    `json:"valor" binding:"required"`
	Type        string `json:"tipo" binding:"required"`
	Description string `json:"descricao" binding:"required"`
	Timestamp   int64
}

func (t *Transaction) GetValue() int {
	if t.Type == "d" {
		return t.Value * -1
	}
	return t.Value
}

func (t *Transaction) Validate() error {
	if t.Type != "d" && t.Type != "c" {
		return fmt.Errorf("invalid type %s", t.Type)
	}
	if t.Value < 0 {
		return fmt.Errorf("invalid value %d", t.Value)
	}
	if len(t.Description) < 1 || len(t.Description) > 10 {
		return fmt.Errorf("invalid description %s", t.Description)
	}
	return nil
}

type Resume struct {
	Balance      int
	Limit        int
	Transactions []*Transaction
}
