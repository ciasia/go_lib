package databath

import ()

type CustomQuery struct {
	Query     string
	InFields  []Field
	OutFields map[string]Field
}
