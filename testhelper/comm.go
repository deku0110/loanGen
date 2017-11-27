package testhelper

import (
	"time"
	loadgenlib "loadgen/lib"
	"math/rand"
	"encoding/json"
	"net"
	"bufio"
	"bytes"
	"fmt"
)

const (
	DELIM = '\n' // 分隔符。
)

type TcpComm struct {
	addr string
}

func NewTCPComm(addr string) loadgenlib.Caller {
	return &TcpComm{addr:addr}
}
//构建一个请求
func (tcp *TcpComm) BuildReq() loadgenlib.RawReq {
	id := time.Now().UnixNano()
	sreq := ServerReq{
		ID: id,
		Operands: []int{
			int(rand.Int31n(1000)), int(rand.Int31n(10000))},
		Operator: func() string {
			op := []string{"+", "-", "*", "/"}
			return op[rand.Int31n(100)%4]
		}(),
	}
	bbytes, err := json.Marshal(sreq)
	if err != nil {
		panic(err)
	}
	rawReq := loadgenlib.RawReq{ID: id, Req: bbytes}
	return rawReq
}
//发起一次通讯
func (tcp *TcpComm) Call(req []byte, timeoutNS time.Duration) ([]byte, error) {
	conn, err := net.DialTimeout("tcp", tcp.addr, timeoutNS)
	if err != nil {
		return nil, err
	}
	_, err = write(conn, req, DELIM)
	if err != nil {
		return nil, err
	}
	return read(conn,DELIM)
}

//检查响应内容
func (tcp *TcpComm) CheckResp(rawReq loadgenlib.RawReq, rawResp loadgenlib.RawResp) *loadgenlib.CallerResult {
	var commResult loadgenlib.CallerResult
	commResult.ID = rawResp.ID
	commResult.Req = rawReq
	commResult.Resp = rawResp
	var sreq ServerReq
	err := json.Unmarshal(rawReq.Req,&sreq)
	if err != nil {
		commResult.Code = loadgenlib.RET_CODE_FATAL_CALL
		commResult.Msg = fmt.Sprintf("Incorrectly formatted Req: %s\n",string(rawReq.Req))
		return &commResult
	}
	var sersp ServerResp
	err = json.Unmarshal(rawResp.Resp, &sersp)
	if err != nil {
		commResult.Code = loadgenlib.RET_CODE_ERROR_RESPONSE
		commResult.Msg =
			fmt.Sprintf("Incorrectly formatted Resp: %s!\n", string(rawResp.Resp))
		return &commResult
	}
	if sersp.ID != sreq.ID {
		commResult.Code = loadgenlib.RET_CODE_ERROR_RESPONSE
		commResult.Msg =
			fmt.Sprintf("Inconsistent raw id! (%d != %d)\n", rawReq.ID, rawResp.ID)
		return &commResult
	}
	if sersp.Err != nil {
		commResult.Code = loadgenlib.RET_CODE_ERROR_CALEE
		commResult.Msg =
			fmt.Sprintf("Abnormal server: %s!\n", sersp.Err)
		return &commResult
	}
	if sersp.Result != op(sreq.Operands, sreq.Operator) {
		commResult.Code = loadgenlib.RET_CODE_ERROR_RESPONSE
		commResult.Msg =
			fmt.Sprintf(
				"Incorrect result: %s!\n",
				genFormula(sreq.Operands, sreq.Operator, sersp.Result, false))
		return &commResult
	}
	commResult.Code = loadgenlib.RET_CODE_SUCCESS
	commResult.Msg = fmt.Sprintf("Success. (%s)", sersp.Formula)
	return &commResult
}

//write 会向连接写数据,并在最后追加参数delim代表的字节
func write(conn net.Conn, content []byte, delim byte) (int, error) {
	writer := bufio.NewWriter(conn)
	n, err := writer.Write(content)
	if err == nil {
		writer.WriteByte(delim)
	}
	if err == nil {
		err = writer.Flush()
	}
	return n, err
}

func read(conn net.Conn, delim byte) ([]byte, error) {
	readBytes := make([]byte,1)
	var buffer bytes.Buffer
	for{
		_,err := conn.Read(readBytes)
		if err != nil {
			return nil,err
		}
		readByte := readBytes[0]
		if readByte == delim {
			break
		}
		buffer.WriteByte(readByte)
	}
	return buffer.Bytes() ,nil
}