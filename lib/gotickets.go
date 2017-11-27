package lib

import (
	"fmt"
	"errors"
)

//GoTickets 表示Goroutine票池的接口
type GoTickets interface {
	//拿走一张票
	Take()
	//归还一张票
	Return()
	//票池是否已被激活
	Active() bool
	//票的总量
	Total() uint32
	//剩余的票数
	Remainder() uint32
}

type myGotickets struct {
	total    uint32        //票的总数
	ticketCh chan struct{} //票的容器
	active   bool          //票池是否被激活
}

func NewGoTicketes(total uint32) (GoTickets, error) {
	gt := myGotickets{}

	if !gt.init(total) {
		errMsg :=
			fmt.Sprintf("The goroutine ticket pool can NOT be initialized! (total=%d)\n", total)
		return nil, errors.New(errMsg)
	}
	return &gt, nil
}

func (gt *myGotickets) init(total uint32) bool {
	if gt.active {
		return false
	}
	if total == 0 {
		return false
	}
	ch := make(chan struct{}, total)
	n := int(total)
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	gt.ticketCh = ch
	gt.total = total
	gt.active = true
	return true
}

func (gt *myGotickets) Take() {
	<-gt.ticketCh
}

func (gt *myGotickets) Return() {
	gt.ticketCh <- struct{}{}
}

func (gt *myGotickets) Active() bool {
	return gt.active
}

func (gt *myGotickets) Total() uint32 {
	return gt.total
}

func (gt *myGotickets) Remainder() uint32 {
	return uint32(len(gt.ticketCh))
}
