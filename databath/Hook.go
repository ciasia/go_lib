package databath

import ()

type Hook struct {
	Collection   string                 `json:"collection"`
	When         HookWhen               `json:"when"`
	Set          map[string]interface{} `json:"set"`
	Email        *HookEmail             `json:"email"`
	Raw          *rawCustomQuery        `json:"raw"`
	Scripts      []string               `json:"scripts"`
	CustomAction *CustomQuery
}

type HookWhen struct {
	Field string `json:"field"`
	What  string `json:"what"`
}

type HookEmail struct {
	Recipient string `json:"recipient"`
	Template  string `json:"template"`
}
