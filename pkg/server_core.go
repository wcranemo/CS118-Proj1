package whatsup

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const BATCH_SIZE = 50
const MAILBOX_SIZE = 1024

// A simple hash function for Connect to use to generate new tokens.
// Not used anywhere else.
func hash(name string) (result string) {
	return fmt.Sprintf("%x", md5.Sum([]byte(name)))
}

// The gRPC implementation of our Server
type Server struct {
	UnimplementedWhatsUpServer
	// A map of auth tokens to strings
	AuthToUserTable map[string]string
	// A map of users to messages in their inbox. The inbox is modelled
	// as a buffered channel of size MAILBOX_SIZE.
	Inboxes map[string](chan *ChatMessage)
}

func NewServer() Server {
	return Server{
		AuthToUserTable: make(map[string]string),
		Inboxes:         make(map[string](chan *ChatMessage)),
	}
}

// A server-side interceptor that maps the authentication tokens in our `context` back to usernames.
// Rejects calls if they don't have a valid authentication token. Note: we've made our interceptor
// in this case a method on our Server struct so that it can have access to the Server's private variables
// - however, this is not a strict requirement for interceptors in general.
func (s Server) Interceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {

	// allow calls to Connect endpoint to pass through
	if info.FullMethod == "/whatsup.WhatsUp/Connect" {
		return handler(ctx, req)
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, errors.New("Couldn't read metadata for request")
	}

	// if token is present in metadata
	if values, ok := md["token"]; ok {
		if len(values) == 1 {
			// if user is present in s.AuthToUserTable
			if user, ok := s.AuthToUserTable[values[0]]; ok {
				return handler(context.WithValue(context.Background(), "username", user), req)
			}
		}
	}

	return nil, errors.New("Could not fetch user from authentication token, if provided")
}

// Implementation of the Connect method defined in our `.proto` file.
// Converts the username provided by `Registration` to an `AuthToken` object.
// The token returned is unique to the user - if the user is already logged in,
// the connect will should be rejected. This function creates a corresponding entry
// in `s.AuthToUserTable` and `s.Inboxes`.
func (s Server) Connect(_ context.Context, r *Registration) (*AuthToken, error) {

	token := hash(r.SourceUser)

	if _, ok := s.AuthToUserTable[token]; !ok {
		s.AuthToUserTable[token] = r.SourceUser
		s.Inboxes[r.SourceUser] = make(chan *ChatMessage, MAILBOX_SIZE)

		return &AuthToken{
			Token: token,
		}, nil
	}

	return nil, errors.New("User is already logged in")

}

// Implementation of the Send method defined in our `.proto` file.
// Should write the chat message to a target user's private inbox in s.Inboxes.
// The chat message should have its `User` field replaced with the sending user
// (when you initially receive it, it will have the name of the recipient instead).
// TODO: Implement `Send`. If any errors occur, return any error message you'd like.
func (s Server) Send(ctx context.Context, msg *ChatMessage) (*Success, error) {

	if msg == nil {
        return nil, errors.New("message is missing")
    }

	// other user has to be logged in to have an inbox
    recipientInbox, ok := s.Inboxes[msg.User]
    if !ok {
        return nil, errors.New("user is not logged in")
    }

	// check valid sender
    sender, ok := ctx.Value("username").(string)
    if !ok {
        return nil, errors.New("could not identify sender")
    }

    outgoing := &ChatMessage{
        User: sender,
        Body: msg.Body,
    }

    select {
		// make sure the message is sent successfully
    case recipientInbox <- outgoing:
        return &Success{Ok: true}, nil
		// throw error otherwise
    case <-ctx.Done():
        return nil, ctx.Err()
    }	
}

// Implementation of the Fetch method defined in our `.proto` file.
// Should consume all messages from the inbox channel for the current user
// in batches of BATCH_SIZE. Hint: use `select` statements in a suitable `for`
// loop to consume from the channel until some condition is reached, since you
// don't want to accidentally miss messages.
//
// TODO: Implement Fetch. If any errors occur, return any error message you'd like.
func (s Server) Fetch(ctx context.Context, _ *Empty) (*ChatMessages, error) {
	// make sure we have a valid current user
	user, ok := ctx.Value("username").(string)
if !ok {
    return nil, errors.New("could not identify current user")
}

	// retrieve the inbox
inbox, ok := s.Inboxes[user]
if !ok {
    return nil, errors.New("user inbox not found")
}

	// initialize an empty Chatmessages to hold the results when we loop through the 
result := &ChatMessages{
        // Message: []*ChatMessage{},
    }

for len(result.Messages) < BATCH_SIZE { //limit the fetch call to BATCH_SIZE # of messages
	// messages will be taken out of channel, so can be taken 
    select {
    case message, open := <-inbox: // keep taking messages from inbox if channel is open
        if !open {
            return result, nil // inbox channel was closed/empty so return results
        }
        result.Messages = append(result.Messages, message)
    default:
        return result, nil // inbox is empty right now
    }
}
return result, nil
}

// Implementation of the List method defined in our `.proto` file.
// Should consume from the inbox channel for the current user.
func (s Server) List(ctx context.Context, _ *Empty) (*UserList, error) {

	u := &UserList{
		Users: []string{},
	}

	for _, user := range s.AuthToUserTable {
		u.Users = append(u.Users, user)
	}

	return u, nil

}

// Implementation of the Disconnect method defined in our `.proto` file.
// Should destroy the corresponding inbox and entry in `s.AuthToUserTable`
func (s Server) Disconnect(ctx context.Context, _ *Empty) (*Success, error) {

	user := fmt.Sprintf("%v", ctx.Value("username"))
	close(s.Inboxes[user]) // make sure no more writes can be sent on this channel
	delete(s.Inboxes, user)

	for token, u := range s.AuthToUserTable {
		if u == user {
			delete(s.AuthToUserTable, token)
		}
	}

	return &Success{Ok: true}, nil
}
