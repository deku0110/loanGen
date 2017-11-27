package loadgen

import (
	"loadgen/lib"
	"time"
	"context"
	"github.com/kataras/golog"
	"bytes"
	"math"
	"fmt"
	"sync/atomic"
	"errors"
)

type myGenerator struct {
	caller      lib.Caller             //调用器
	timeoutNS   time.Duration          //处理超时时间 单位:纳秒
	lps         uint32                 //每秒载荷量
	durationNS  time.Duration          //负载持续时间
	concurrency uint32                 //载荷并发量
	tickets     lib.GoTickets          //Goroutine票池
	ctx         context.Context        //上下文
	cancelFunc  context.CancelFunc     //取消函数
	callCount   int64                  //调用计数
	status      uint32                 //状态
	resultCH    chan *lib.CallerResult //调用结果通道
}

func NewMyGenerator(pset ParamSet) (lib.Generator, error) {

	golog.Info("New a load generator")
	if err := pset.Check(); err != nil {
		return nil, err
	}
	gen := &myGenerator{
		caller:     pset.Caller,
		timeoutNS:  pset.TimeoutNS,
		lps:        pset.LPS,
		durationNS: pset.DurationNS,
		status:     lib.STATUS_ORIGINAL,
		resultCH:   pset.ResultCh,
	}
	if err := gen.init(); err != nil {
		return nil, err
	}
	return gen, nil
}

func (gen *myGenerator) init() error {
	var buf bytes.Buffer
	buf.WriteString("Initializing the load generator...")
	//载荷的并发量 ≈ 载荷的响应超时时间 / 载荷的发送间隔时间
	var total64 = int64(gen.timeoutNS)/int64(1e9/gen.lps) + 1
	if total64 > math.MaxInt32 {
		total64 = math.MaxInt32
	}
	gen.concurrency = uint32(total64)
	tickets, err := lib.NewGoTicketes(gen.concurrency)
	if err != nil {
		return err
	}
	gen.tickets = tickets

	buf.WriteString(fmt.Sprintf("Done. (concurrency=%d", gen.concurrency))
	golog.Info(buf.String(), "\n")
	return nil
}

func (gen *myGenerator) Start() bool {
	golog.Info("Starting load generator...")
	//检查是否具有可启动状态,顺便设置状态为正在启动
	if !atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_ORIGINAL, lib.STATUS_STARTING) {
		if !atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_STOPPED, lib.STATUS_STARTING) {
			return false
		}
	}

	//设定节流阀
	var throttle <-chan time.Time
	if gen.lps > 0 {
		interval := time.Duration(1e9 / gen.lps)
		golog.Infof("Setting throttle (%v)...", interval)
		throttle = time.Tick(interval)
	}

	//初始化上下文和取消函数
	gen.ctx, gen.cancelFunc = context.WithTimeout(context.Background(), gen.durationNS)

	// 初始化调用计数
	gen.callCount = 0

	//设置状态为已启动
	atomic.StoreUint32(&gen.status, lib.STATUS_STARTED)

	go func() {
		//生成并发送载荷
		golog.Info("Generating loads...")
		gen.genLoad(throttle)
		golog.Info("Stopped. (call count : %d", gen.callCount)
	}()
	return true
}

func (gen *myGenerator) genLoad(throttle <-chan time.Time) {
	for {
		select {
		case <-gen.ctx.Done():
			gen.prepareToSop(gen.ctx.Err())
			return
		default:
		}
		gen.asyncCall()
		if gen.lps > 0 {
			select {
			case <-throttle:
			case <-gen.ctx.Done():
				gen.prepareToSop(gen.ctx.Err())
				return
			}
		}
	}
}

//用于为停止载荷发生器做准备
func (gen *myGenerator) prepareToSop(ctxError error) {
	golog.Info("Prepare to stop load generator (cause: %s)...", ctxError)
	atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_STARTED, lib.STATUS_STOPPING)
	golog.Infof("Closing result channel...")
	close(gen.resultCH)
	atomic.StoreUint32(&gen.status, lib.STATUS_STOPPED)
}

func (gen *myGenerator) asyncCall() {
	gen.tickets.Take()
	go func() {
		if p := recover(); p != nil {
			err, ok := interface{}(p).(error)
			var errMsg string
			if ok {
				errMsg = fmt.Sprintf("Async Call Panic! (error: %s)", err)
			} else {
				errMsg = fmt.Sprintf("Async Call Panic! (clue: %#v)", p)
			}
			golog.Error(errMsg, "\n")
			result := &lib.CallerResult{
				ID:   -1,
				Code: lib.RET_CODE_FATAL_CALL,
				Msg:  errMsg,
			}
			gen.sendResult(result)
		}
		gen.tickets.Return()
	}()
	rawReq := gen.caller.BuildReq()
	//调用状态 0-未动用或调用中 1-调用完成;2-调用超时
	var callStatus uint32
	timer := time.AfterFunc(gen.timeoutNS, func() {
		if !atomic.CompareAndSwapUint32(&callStatus, 0, 2) {
			return
		}
		result := &lib.CallerResult{
			ID:     rawReq.ID,
			Req:    rawReq,
			Code:   lib.RET_CODE_WARNING_CALL_TIMEOUT,
			Msg:    fmt.Sprintf("Timeout! (expected: < %v)", gen.timeoutNS),
			Elapse: gen.timeoutNS,
		}
		gen.sendResult(result)
	})
	rawResp := gen.callOne(&rawReq)
	if !atomic.CompareAndSwapUint32(&callStatus, 0, 1) {
		return
	}
	timer.Stop()
	var result *lib.CallerResult
	if rawResp.Err != nil {
		result = &lib.CallerResult{
			ID:     rawResp.ID,
			Req:    rawReq,
			Code:   lib.RET_CODE_ERROR_CALL,
			Msg:    rawResp.Err.Error(),
			Elapse: rawResp.Elapse,
		}
	} else {
		result = gen.caller.CheckResp(rawReq, *rawResp)
		result.Elapse = rawResp.Elapse
	}
	gen.sendResult(result)
}

func (gen *myGenerator) sendResult(result *lib.CallerResult) bool {
	if atomic.LoadUint32(&gen.status) != lib.STATUS_STARTED {
		gen.printIgnoredResult(result, "stopped load generator")
		return false
	}
	select {
	case gen.resultCH <- result:
		return true
	default:
		gen.printIgnoredResult(result, "full result channel")
		return false
	}
}

// callOne 会向载荷承受方发起一次调用。
func (gen *myGenerator) callOne(rawReq *lib.RawReq) *lib.RawResp {
	atomic.AddInt64(&gen.callCount, 1)
	if rawReq == nil {
		return &lib.RawResp{ID: -1, Err: errors.New("Invalid raw request.")}
	}
	start := time.Now().UnixNano()
	resp, err := gen.caller.Call(rawReq.Req, gen.timeoutNS)
	end := time.Now().UnixNano()
	elaspedTime := time.Duration(end - start)
	var rawResp lib.RawResp
	if err != nil {
		errMsg := fmt.Sprintf("Sync Call Error: %s.", err)
		rawResp = lib.RawResp{
			ID:     rawReq.ID,
			Err:    errors.New(errMsg),
			Elapse: elaspedTime,
		}
	} else {
		rawResp = lib.RawResp{
			ID:     rawReq.ID,
			Resp:   resp,
			Elapse: elaspedTime,
		}
	}
	return &rawResp
}

// printIgnoredResult 打印被忽略的结果。
func (gen *myGenerator) printIgnoredResult(result *lib.CallerResult, cause string) {
	resultMsg := fmt.Sprintf(
		"ID=%d, Code=%d, Msg=%s, Elapse=%v",
		result.ID, result.Code, result.Msg, result.Elapse)
	golog.Warnf("Ignored result: %s. (cause: %s)\n", resultMsg, cause)
}

func (gen *myGenerator) Stop() bool {
	if !atomic.CompareAndSwapUint32(&gen.status, lib.STATUS_STARTED, lib.STATUS_STOPPING) {
		return false
	}
	gen.cancelFunc()
	for {
		if atomic.LoadUint32(&gen.status) == lib.STATUS_STOPPED {
			break
		}
		time.Sleep(time.Microsecond)
	}
	return true
}

func (gen *myGenerator) Status() uint32 {
	return atomic.LoadUint32(&gen.status)
}

func (gen *myGenerator) CallCount() int64 {
	return atomic.LoadInt64(&gen.callCount)
}
