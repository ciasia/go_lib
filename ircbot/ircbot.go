// IRC Bot generic implementation
//
// Damien Whitten 2013 - github.com/daemonl

package ircbot

import (
	"bufio"
	"fmt"

	"net"
	"regexp"
	"strings"
	"time"
)

const (
	RPL_WELCOME = "001" //The first message sent after client registration. The text used varies widely
	RPL_AWAY    = "301" //Used in reply to a command directed at a user who is marked as away
)

// Matches (hopefully) all IRC messages.
var messageRegex *regexp.Regexp = regexp.MustCompile(`^(?:\:(\S+) )?(\S+)(?: (.*))? :(.*)`)

// Connection holds a connection to an IRC server.
type Connection struct {
	conn               net.Conn
	reader             *bufio.Reader
	Config             *Config
	Chans              map[string]*IrcChan
	Conversations      map[string]*Conversation
	initChan           chan bool
	hasBeenInitialised bool
	outChan            chan string
}

// IrcChan represents a joined channel on the IRC server.
type IrcChan struct {
	Connection *Connection
	name       string
	Chan       chan Message
}

// Conversation represents a private conversation with a nick.
type Conversation struct {
	Connection *Connection
	who        string
	Nick       string
	Chan       chan Message
}

// Message is a broken down structure of an IRC server message.
type Message struct {
	Raw          string
	Who          string
	Command      string
	Parameters   string
	Content      string
	SenderNick   string
	SenderServer string
	Time         time.Time
}

// Config is to be passed through to the connection
type Config struct {
	Address        string
	Nick           string
	Password       string
	OnConversation func(*Conversation)
}

// Connection.Connect establishes a connection with an IRC server,
// waits for a response, loggs in, waits for welcome then returns control.
// TODO: Add Timeout on 'welcome' wait.
// TODO: Handle exceptions like nick taken.
func (c *Connection) Connect() error {
	c.Chans = make(map[string]*IrcChan)
	c.Conversations = make(map[string]*Conversation)

	c.outChan = make(chan string)
	conn, err := net.Dial("tcp", c.Config.Address)

	if err != nil {
		return err
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	ln, err := c.reader.ReadString('\n')
	if err != nil {
		fmt.Println("ERROR:" + err.Error())
		return err
	}
	fmt.Println(ln)

	time.Sleep(time.Millisecond * 100)

	go c.writeLoop()

	c.Writef("PASS %s", c.Config.Password)
	c.Writef("NICK %s", c.Config.Nick)
	c.Writef("USER %s 8 * : gopher bot", c.Config.Nick)

	go c.readLoop()

	c.initChan = make(chan bool)
	_ = <-c.initChan

	return nil
}

func (c *Connection) readLoop() {
	for {
		line, err := c.reader.ReadString('\n')
		if err != nil {
			c.Close()
			err2 := c.Connect()
			if err2 != nil {
				panic(err2)
			}
		}
		if len(line) < 1 {
			continue
		}
		line = line[:len(line)-1]
		//fmt.Println(line)
		parts := messageRegex.FindStringSubmatch(line)
		if len(parts) != 5 {
			//fmt.Println("INVALID LINE: " + line)
			continue
		}
		m := Message{
			Raw:        parts[0],
			Who:        parts[1],
			Command:    parts[2],
			Parameters: parts[3],
			Content:    parts[4][:len(parts[4])-1],
		}

		go c.EnactMessage(m)
	}
}

func (c *Connection) writeLoop() {
	for {
		line := <-c.outChan
		fmt.Println("OUT: ", line)
		c.conn.Write([]byte(line + "\r\n"))
		time.Sleep(time.Second * 1) // Rate limit
	}
}

// Connection.Write sends the line string to the IRC server, adding line endings
func (c *Connection) Write(line string) {
	fmt.Println("QUE: ", line)
	c.outChan <- line
}

func (c *Connection) Writef(format string, args ...interface{}) {
	line := fmt.Sprintf(format, args...)
	c.Write(line)
}

func (c *Connection) EnactMessage(m Message) {

	whoParts := strings.Split(m.Who, "!")
	if len(whoParts) == 2 {
		m.SenderNick = whoParts[0]
		m.SenderServer = whoParts[1]
	}
	m.Time = time.Now()

	fmt.Printf("IN: %s\n", m.Raw)

	switch m.Command {
	case RPL_WELCOME:
		if c.hasBeenInitialised {
			return
		}
		c.initChan <- true
	case "PING":
		c.Write("PONG" + m.Raw[4:])
	case "PRIVMSG":
		if m.Parameters[0] == '#' {
			ircChan := c.Chans[m.Parameters[1:]]
			if ircChan != nil {
				ircChan.GotMessage(m)
			}
		}

		if m.Parameters == c.Config.Nick {
			_, ok := c.Conversations[m.Who]
			if !ok {
				newConversation := Conversation{Connection: c, who: m.Who, Nick: m.SenderNick, Chan: make(chan Message)}
				c.Conversations[m.Who] = &newConversation
				if c.Config.OnConversation != nil {
					go c.Config.OnConversation(&newConversation)
				}
			}
			c.Conversations[m.Who].GotMessage(m)
		}

	}
}

func (c *Connection) Close() {
	c.Write("QUIT :bot out")
	//*c.conn.Close()
}

func (c *Connection) Join(channel string) *IrcChan {
	c.Writef("JOIN #%s", channel)
	ch := IrcChan{Connection: c, name: channel, Chan: make(chan Message)}
	c.Chans[channel] = &ch
	return &ch
}

func (c *IrcChan) Leave() {
	c.Connection.Writef("LEAVE #%s", c.name)
	c.Connection.Chans[c.name] = nil
}

func (c *Connection) Send(channel string, message string) {
	c.Writef("PRIVMSG %s :%s", channel, message)
}

func (c *IrcChan) Send(message string) {
	c.Connection.Send("#"+c.name, message)
}

func (c *IrcChan) GotMessage(m Message) {
	c.Chan <- m
}

func (c *Conversation) GotMessage(m Message) {
	c.Chan <- m
}

func (c *Conversation) Send(message string) {
	c.Connection.Send(c.Nick, message)
}
