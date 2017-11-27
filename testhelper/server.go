package testhelper

import (
	"bytes"
	"strconv"
	"net"
	"sync/atomic"
	"errors"
	"github.com/kataras/golog"
	"fmt"
	"encoding/json"
)

type ServerReq struct {
	ID       int64
	Operands []int  //多个运算符
	Operator string //一个匀速那副
}

type ServerResp struct {
	ID      int64
	Formula string
	Result  int
	Err     error
}

// TCPServer 表示基于TCP协议的服务器。
type TCPServer struct {
	listener net.Listener
	active   uint32 // 0-未激活；1-已激活。
}

// NewTCPServer 会新建一个基于TCP协议的服务器。
func NewTCPServer() *TCPServer {
	return &TCPServer{}
}

func op(operands []int, operator string) int {
	var result int
	switch {
	case operator == "+":
		for _, v := range operands {
			if result == 0 {
				result = v
			} else {
				result += v
			}
		}
	case operator == "-":
		for _, v := range operands {
			if result == 0 {
				result = v
			} else {
				result -= v
			}
		}
	case operator == "*":
		for _, v := range operands {
			if result == 0 {
				result = v
			} else {
				result *= v
			}
		}
	case operator == "/":
		for _, v := range operands {
			if result == 0 {
				result = v
			} else {
				result /= v
			}
		}
	}
	return result
}

// genFormula 会根据参数生成字符串形式的公式。
func genFormula(operands []int, operator string, result int, equal bool) string {
	var buff bytes.Buffer
	n := len(operands)
	for i := 0; i < n; i++ {
		if i > 0 {
			buff.WriteString(" ")
			buff.WriteString(operator)
			buff.WriteString(" ")
		}

		buff.WriteString(strconv.Itoa(operands[i]))
	}
	if equal {
		buff.WriteString(" = ")
	} else {
		buff.WriteString(" != ")
	}
	buff.WriteString(strconv.Itoa(result))
	return buff.String()
}

// Close 会关闭服务器。
func (server *TCPServer) Close() bool {
	if !atomic.CompareAndSwapUint32(&server.active, 1, 0) {
		return false
	}
	server.listener.Close()
	return true
}
func (server *TCPServer) init(addr string) error {
	if !atomic.CompareAndSwapUint32(&server.active, 0, 1) {
		return errors.New("init TCPServer failed")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		atomic.StoreUint32(&server.active, 0)
		return err
	}
	server.listener = ln
	return nil
}
func (server *TCPServer) Listen(addr string) error {
	err := server.init(addr)
	if err != nil {
		return err
	}
	go func() {
		for {
			if atomic.LoadUint32(&server.active) != 1 {
				break
			}
			conn, err := server.listener.Accept()
			if err != nil {
				if atomic.LoadUint32(&server.active) == 1 {
					golog.Errorf("Server Request Acception Error: %s\n", err)
				} else {
					golog.Warnf("Server :Broken Acception because of closed network connection.")
				}
				continue
			}
			go reqHandler(conn)
		}
	}()
	return nil
}

func reqHandler(conn net.Conn) {
	var errMsg string
	var sresp ServerResp
	req, err := read(conn, DELIM)
	if err != nil {
		errMsg = fmt.Sprintf("Server: Req Read Error: %s",err)
	} else {
		var sreq ServerReq
		err := json.Unmarshal(req, &sreq)
		if err != nil {
			errMsg = fmt.Sprintf("Server: Req Unmarshal Error: %s", err)
		} else {
			sresp.ID = sreq.ID
			sresp.Result = op(sreq.Operands,sreq.Operator)
			sresp.Formula = genFormula(sreq.Operands,sreq.Operator,sresp.Result,true)
		}
	}
	if errMsg != "" {
		sresp.Err = errors.New(errMsg)
	}
	bbytes, err := json.Marshal(sresp)
	if err != nil {
		golog.Errorf("Server: Resp Marshal Error:%s",err)
	}
	_,err = write(conn,bbytes,DELIM)
	if err != nil {
		golog.Errorf("Server: Resp Write error: %s", err)
	}
}
