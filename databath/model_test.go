package databath

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"testing"
)

func TestPlaceholder(t *testing.T) {

	jsonReader := getTestModelStream()
	model, err := ReadModelFromReader(jsonReader)

	if err != nil {
		switch e := err.(type) {
		case *json.SyntaxError:
			fmt.Printf("Syntax Error at %d\n", e.Offset)
		case *json.UnmarshalTypeError:
			fmt.Printf("Unmarshal Type Error %s (%s <- %s) \n", e.Error(), e.Type.Name(), e.Value)

		default:
			fmt.Println(err)
		}
	}

	fmt.Println(model)

	_ = model

}

func getTestModelStream() io.ReadCloser {
	jsonBlob := `{
	"collections":
{
  "session": {

    "fields": {
      "id":         {"type": "id"},
      "user":       {"type": "ref", "collection": "staff"},
      "flash":      {"type": "array"},
      "last":       {"type": "datetime"}
    },
    "fieldsets": {
      "application": ["id", "user", "flash", "last"],
      "identity": ["id"]
    }
  },

   "history": {
    "fields": {
      "id": {"type": "id"},
      "user": {"type": "ref", "collection": "staff"},
      "timestamp": {"type": "datetime"},
      "identity": {"type": "string"},
      "action": {"type": "string"},
      "entity_id": {"type": "int"},
      "entity": {"type": "string"},
      "changeset": {"type": "text"}
    },
    "fieldsets": {
        "table": ["user.username", "identity", "timestamp", "action", "entity", "entity_id"],
        "identity": ["id"]
    }
  },
  
  "staff": {
    "fields": {
      "id":      {"type": "id"},
      "username":     {"type": "string", "length": 255},
      "password": {"type": "string", "length": 255}
    },
    "fieldsets":{
      "default": ["id", "username"],
      "application": ["id","username", "password"],
      "login": ["id", "username", "password"],
      "identity": ["username"]
    },
    "masks": {
      "default": {"read": "default", "write": []},
      "admin": {"read": "default", "write": "default"},
      "owner": {"read": "default", "write": "default"}
    }
  },

  "customer": {
    "fields": {
      "id": {"type": "id"},
      "name": {"type": "string", "important": true},
      "address": {"type": "address"},
      "phones": 
        {
          "type": "array", "fields": 
          {
            "type":   {"length": 20, "type": "enum", "choices": {"phone": "Phone", "fax": "Fax"} },
            "number": {"length": 40, "type": "string"}
          }
        },
      "notes": {"type": "text"}
    },
    "fieldsets": {
      "table": ["name", "address"],
      "form": ["name", "address", "phones", "notes"]
    }
  },

  "project": {
    "fields": {
      "id": {"type": "id"},
      "customer": {"type": "ref", "collection": "customer"},
      "name": {"type": "string", "max_length": 200, "important": true },
      "contact": {"type": "ref", "collection": "person"},
      "rate": {"type": "float"},
      "flat": {"type": "float"},
      "status": {"type": "enum", "important": true, "choices": 
        {
          "10": "Open", 
          "20": "Quoting",
          "30": "Accepted",
          "40": "In Progress", 
          "50": "Completed", 
          "60": "Closed"
        }
      },
      "description": {"type": "text"},
      "solution": {"type": "text"},
      "created": {"type": "auto_timestamp", "bind": "create"},
      "modified": {"type": "auto_timestamp", "bind": "modify"}
    },
    "fieldsets": {
      "table": [
          "name", 
          "status", 
          "customer.name",
          "rate",
          "flat",
          {
            "label": "Time Spent",
            "type": "totalduration",
            "dataType": "number",
            "path": "time_session.time_spent",
            "start": "start",
            "stop": "end"
          }
        ],
        "display": [
          "customer",
          "customer.name",
          "name",
          "contact.name_given",
          "status",
          "description",
          "solution"
 
        ],


      "form": [
        "customer",
        "customer.name",
        "name",
        "contact.name_given",
        "status",
        "description",
        "solution",
        "created",
        "modified",
        "rate",
        "flat"
      ]
    }
  },

  "person": {
    "table": "person",
    "pk": "id",
    "fields": {
      "id": {"type": "id"},
      "name_given": {"type": "string", "important": true},
      "name_given_other": {"type": "string"},
      "name_family": {"type": "string", "important": true},
      "email": {"type": "string"},
      "customer": {"type": "ref", "collection": "customer"},
      "phones": {"type": "array", "fields": {"type": {"type": "string"}, "number": {"type": "string"}}}
    },
    "fieldsets":{
      "table": ["name_given", "name_family", "customer.name"],
      "form": ["name_given", "name_given_other", "name_family", "customer", "customer.name", "email", "phones"],
      "identity": ["name_given", "name_family"]
    }
  },

  "time_session": {
    "table": "time_session",
    "pk": "id",
    "fields": {
      "id": {"type": "id"},
      "start": {"type": "datetime"},
      "end": {"type": "datetime"},
      "project": {"type": "ref", "collection": "project"},
      "notes": {"type": "text"}
    },
    "fieldsets": {
      "table": ["project.name", "start", "end"],
      "form": ["project", "project.name", "start", "end", "notes"],
      "identity": ["project.name"]
    }
    
  }


}
}
	`

	r := bytes.NewReader([]byte(jsonBlob))
	rc := ioutil.NopCloser(r)

	return rc
}
