# MyMembers

An implementation of the [SWIM protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf) in Go. This is a learning project and is not designed for production use.


## Architecture

<img width="854" height="720" alt="Screenshot 2026-02-27 at 11 25 40 PM" src="https://github.com/user-attachments/assets/3694590c-a4fa-44f0-b3b4-87770faa30fa" />


## Example

```go
// Create node 1
node1Config := DefaultConfig()
node1Config.Name = "node1"
node1Config.BindAddr = "127.0.0.1"
node1Config.BindPort = 6969

node1, _ := Create(node1Config)

// Create node 2
node2Config := DefaultConfig()
node2Config.Name = "node2"
node2Config.BindAddr = "127.0.0.1"
node2Config.BindPort = 6970

node2, _ := Create(node2Config)

// Node 2 joins node 1
node2.Join([]string{"127.0.0.1:6969"})

// Check members
fmt.Println(len(node1.Members())) // 2
fmt.Println(len(node2.Members())) // 2

// Graceful leave
node2.Leave()
node2.Shutdown()
```

## Learning Resources

- **SWIM Paper:** [SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol](https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf)
- **memberlist** (HashiCorp): https://github.com/hashicorp/memberlist
- **ringpop-go** (Uber): https://github.com/uber/ringpop-go

## Contact

Pawan Mehta — arowpk@gmail.com
