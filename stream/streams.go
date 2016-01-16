package stream

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
)

type emptyStream struct{}

func (self emptyStream) Next() (Event, error) {
	return nil, EOI
}

func (self emptyStream) Len() int {
	return 0
}

func (self emptyStream) Drain() {}

// Create an empty stream.
func Empty() Stream {
	return emptyStream{}
}

type listStream struct {
	events []Event
	num    int
}

func (self *listStream) Len() int {
	return len(self.events)
}

func (self *listStream) Next() (Event, error) {
	if self.num >= len(self.events) {
		return nil, EOI
	}

	self.num += 1
	return self.events[self.num-1], nil
}

func (self *listStream) Drain() {
	self.num = len(self.events)
	self.events = nil
}

// Create a stream from the array of events.
func List(events []Event) Stream {
	return &listStream{events, 0}
}

type chanStream chan Event

func (self chanStream) Next() (Event, error) {
	evt, ok := <-self
	if !ok {
		return nil, EOI
	}

	return evt, nil
}

func (self chanStream) Drain() {
	close(self)
}

// Create a stream from a channel.
func Chan(ch chan Event) Stream {
	return chanStream(ch)
}

/*
StreamMultiplexer is a helper structure to create multiple streams that can pull from one base stream.

Typical usage is:

	mp := Multiplexer(s)
	copy1 := mp.New()
	copy2 := mp.New()
	copy3 := mp.New()

	for {
		evt1, _ := copy1.Next()
		evt2, _ := copy2.Next()
		evt3, _ := copy3.Next()
		// here evt1 == evt2 == evt3
	}

It is unsafe to use the original stream after it was passed to a multiplexer.

StreamMultiplexer is implemented using buffers, so it consumes amount of memory linear to the number of copies
and linear to the amount of difference in number of pulled events from copies.
Basically, if you create two copies, drain first one and don't touch the second, then the multiplexer will have
a buffer with all the events you have pulled, so you could pull them from the seond copy.

TODO: maby we should implement single buffer for all the copies and each copy would have an index into that buffer.
*/
type StreamMultiplexer struct {
	stream Stream
	queues [][]Event
	err    error
	maxLen int
}

// Create a stream that pulls from a base stream.
func (self *StreamMultiplexer) New() Stream {
	self.queues = append(self.queues, make([]Event, 0))
	return multiplexedStream{self, len(self.queues) - 1}
}

func (self *StreamMultiplexer) next(num int) (Event, error) {
	queue := self.queues[num]
	if len(queue) > 0 {
		res := queue[0]
		self.queues[num] = queue[1:]
		return res, nil
	}

	if self.err != nil {
		return nil, self.err
	}

	res, err := self.stream.Next()
	if err != nil {
		self.err = err
		fmt.Printf("StreamMultiplexer: max len is %v\n", self.maxLen)
		return nil, err
	}

	if len(self.queues) > 1 {
		for i, v := range self.queues {
			if i != num {
				self.queues[i] = append(v, res)
				nl := len(self.queues[i])
				if nl > self.maxLen {
					self.maxLen = nl
				}
			}
		}
	}

	return res, nil
}

type multiplexedStream struct {
	mp  *StreamMultiplexer
	num int
}

func (self multiplexedStream) Next() (Event, error) {
	return self.mp.next(self.num)
}

// Create a multiplexer from a stream.
func Multiplexer(stream Stream) *StreamMultiplexer {
	return &StreamMultiplexer{stream, make([][]Event, 0), nil, 0}
}

type zipStream struct {
	streams []Stream
}

func (self zipStream) Next() (Event, error) {
	var res []Event = nil
	for i, s := range self.streams {
		evt, err := s.Next()
		if err != nil {
			return nil, err
		}

		if res == nil {
			res = make([]Event, len(self.streams))
		}
		res[i] = evt
	}
	return res, nil
}

/*
Zip multiple streams into one stream, that will yield arrays of events from all these streams.

The resulting stream will end as soon as the first of these base streams.
*/
func Zip(streams ...Stream) Stream {
	return zipStream{streams}
}

type mapStream struct {
	stream Stream
	fn     func(Event) (Event, error)
}

func (self mapStream) Next() (Event, error) {
	evt, err := self.stream.Next()
	if err != nil {
		return nil, err
	}

	return self.fn(evt)
}

// Map a function over a stream.
func Map(stream Stream, fn func(Event) (Event, error)) Stream {
	return mapStream{stream, fn}
}

func getFieldImplRec(evt Event, field []string) (interface{}, bool) {
	if len(field) == 0 {
		return evt, true
	}

	smap, ok := evt.(map[string]interface{})
	if !ok {
		return nil, false
	}

	next, ok := smap[field[0]]
	if !ok {
		return nil, false
	}

	if len(field) == 1 {
		return next, true
	}
	return getFieldImplRec(next, field[1:])
}

func getFieldImpl(evt Event, field string) (interface{}, bool) {
	if field == "" {
		return evt, true
	}

	var arr []string
	if field == "" {
		arr = []string{}
	} else {
		arr = strings.Split(field, ".")
	}
	return getFieldImplRec(evt, arr)
}

/*
Creates a stream with events which are values of a field in events of the original stream as in JSON.
Returns an error if the original field doesn't exist.

The field might be deep inside, as in "object.value.data".
*/
func GetField(stream Stream, field string) Stream {
	return Map(stream, func(evt Event) (Event, error) {
		res, ok := getFieldImpl(evt, field)
		if !ok {
			return nil, errors.New(fmt.Sprintf("GetField: Expected event to have field %s, got %v", field, evt))
		}
		return res, nil
	})
}

func setFieldImplRec(evt Event, field []string, val Event) (FArg, bool) {
	if len(field) == 0 {
		return nil, false
	}

	smap, ok := evt.(map[string]interface{})
	if !ok {
		return nil, false
	}

	if len(field) == 1 {
		res := map[string]interface{}{}
		for k, v := range smap {
			res[k] = v
		}
		res[field[0]] = val
		return res, true
	}

	next, ok := smap[field[0]]
	if !ok {
		rv, ok := setFieldImplRec(map[string]interface{}{}, field[1:], val)
		if !ok {
			return nil, false
		}

		res := map[string]interface{}{}
		for k, v := range smap {
			res[k] = v
		}
		res[field[0]] = rv
		return res, true
	}

	res, ok := setFieldImplRec(next, field[1:], val)
	if ok {
		smap[field[0]] = res
	}

	return evt, ok
}

func setFieldImpl(evt Event, field string, val Event) (FArg, bool) {
	if field == "" {
		return val, true
	}

	var arr []string
	if field == "" {
		arr = []string{}
	} else {
		arr = strings.Split(field, ".")
	}

	res, ok := setFieldImplRec(evt, arr, val)
	return res, ok
}

type setFieldStream struct {
	datas Stream
	vals  Stream
	field string
}

func (self setFieldStream) Next() (Event, error) {
	data, err1 := self.datas.Next()
	val, err2 := self.vals.Next()
	if err1 != nil {
		return nil, err1
	}
	if err2 != nil {
		return nil, err2
	}

	if res, ok := setFieldImpl(data, self.field, val); ok {
		return res, nil
	} else {
		return data, nil
	}
}

/*
Creates a stream with events which are events from a first stream with given field set with values from events of the second stream as in JSON.

The field might be deep inside, as in "object.value.data".
*/
func SetField(datas Stream, vals Stream, field string) Stream {
	return setFieldStream{datas, vals, field}
}

// Creates a boolean stream with true events when the event of an original stream is equal to a given value and false events otherwise.
func EqVal(stream Stream, evt Event) Stream {
	return Map(stream, func(e Event) (Event, error) {
		return reflect.DeepEqual(e, evt), nil
	})
}

// Creates a boolean stream with false events when the event of an original stream is equal to a given value and true events otherwise.
func NeqVal(stream Stream, evt Event) Stream {
	return Map(stream, func(e Event) (Event, error) {
		return !reflect.DeepEqual(e, evt), nil
	})
}

func getIntOrFloat(arg FArg) (float64, bool) {
	rint64, ok := arg.(int64)
	if ok {
		return float64(rint64), true
	}

	rfloat64, ok := arg.(float64)
	if ok {
		return rfloat64, true
	}

	rint, ok := arg.(int)
	if ok {
		return float64(rint), true
	}

	rint32, ok := arg.(int32)
	if ok {
		return float64(rint32), true
	}

	rfloat32, ok := arg.(float32)
	if ok {
		return float64(rfloat32), true
	}

	return 0, false
}

/*
Creates a boolean stream with true events when the event of an original stream is more than a given value and false events otherwise.

Original stream must consist of numbers.
*/
func MoreVal(stream Stream, val float64) Stream {
	return Map(stream, func(e Event) (Event, error) {
		v, ok := getIntOrFloat(e)
		if !ok {
			return nil, errors.New(fmt.Sprintf("MoreVal: Expected event to be number, got %v", e))
		}

		return v > val, nil
	})
}

/*
Creates a boolean stream with true events when the event of an original stream is more or equal to a given value and false events otherwise.

Original stream must consist of numbers.
*/
func MoreEqVal(stream Stream, val float64) Stream {
	return Map(stream, func(e Event) (Event, error) {
		v, ok := getIntOrFloat(e)
		if !ok {
			return nil, errors.New(fmt.Sprintf("MoreEqVal: Expected event to be number, got %v", e))
		}

		return v >= val, nil
	})
}

/*
Creates a boolean stream with true events when the event of an original stream is less than a given value and false events otherwise.

Original stream must consist of numbers.
*/
func LessVal(stream Stream, val float64) Stream {
	return Map(stream, func(e Event) (Event, error) {
		v, ok := getIntOrFloat(e)
		if !ok {
			return nil, errors.New(fmt.Sprintf("LessVal: Expected event to be number, got %v", e))
		}

		return v < val, nil
	})
}

/*
Creates a boolean stream with true events when the event of an original stream is less or equal to a given value and false events otherwise.

Original stream must consist of numbers.
*/
func LessEqVal(stream Stream, val float64) Stream {
	return Map(stream, func(e Event) (Event, error) {
		v, ok := getIntOrFloat(e)
		if !ok {
			return nil, errors.New(fmt.Sprintf("LessEqVal: Expected event to be number, got %v", e))
		}

		return v <= val, nil
	})
}

type orStream struct {
	streams []Stream
}

func (self orStream) Next() (Event, error) {
	ok := false
	var err error = nil
	for _, s := range self.streams {
		val, err1 := s.Next()
		if err != nil {
			continue
		}

		if err1 != nil {
			err = err1
			continue
		}

		bval, bok := val.(bool)
		if !bok {
			err = errors.New(fmt.Sprintf("Or: Expected bool event, got %v", val))
			continue
		}

		ok = ok || bval
	}

	if err != nil {
		return false, err
	}
	return ok, nil
}

/*
Takes multiple boolean streams and creates a boolean stream, or-ing events from original streams.
*/
func Or(streams ...Stream) Stream {
	return orStream{streams}
}

type andStream struct {
	streams []Stream
}

func (self andStream) Next() (Event, error) {
	ok := true
	var err error = nil
	for _, s := range self.streams {
		val, err1 := s.Next()
		if err != nil {
			continue
		}

		if err1 != nil {
			err = err1
			continue
		}

		bval, bok := val.(bool)
		if !bok {
			err = errors.New(fmt.Sprintf("And: Expected bool event, got %v", val))
			continue
		}

		ok = ok && bval
	}

	if err != nil {
		return false, err
	}
	return ok, nil
}

/*
Takes multiple boolean streams and creates a boolean stream, and-ing events from original streams.
*/
func And(streams ...Stream) Stream {
	return andStream{streams}
}

type filterStream struct {
	data  Stream
	flags Stream
}

func (self filterStream) Next() (Event, error) {
	for {
		val, err1 := self.data.Next()
		flag, err2 := self.flags.Next()
		if err1 != nil {
			return nil, err1
		}
		if err2 != nil {
			return nil, err2
		}

		bflag, bok := flag.(bool)
		if !bok {
			return nil, errors.New(fmt.Sprintf("Filter: Expected bool event, got %v", flag))
		}

		if bflag {
			return val, nil
		}
	}
}

/*
Takes a data stream and flags stream and produces a stream with events from the data stream for which corresponding flag is true.
*/
func Filter(stream Stream, flags Stream) Stream {
	return filterStream{stream, flags}
}

type maxByStream struct {
	datas Stream
	vals  Stream

	data Event
	val  float64
	done bool
}

func (self *maxByStream) Next() (Event, error) {
	if self.done {
		return nil, EOI
	}

	for {
		data, err1 := self.datas.Next()
		v, err2 := self.vals.Next()
		if err1 == EOI && err2 == EOI {
			self.done = true
			break
		}

		if err1 != nil {
			return nil, err1
		}
		if err2 != nil {
			return nil, err2
		}

		val, ok := getIntOrFloat(v)
		if !ok {
			return nil, errors.New(fmt.Sprintf("MaxBy: Expected number event, got %v", v))
		}

		if val > self.val {
			self.data = data
			self.val = val
		}
	}
	return self.data, nil
}

/*
Takes a data stream and numbers stream and produces a stream with event from the data stream for which corresponding value is maximal.
*/
func MaxBy(datas Stream, vals Stream) Stream {
	return &maxByStream{datas, vals, nil, -math.MaxFloat64, false}
}

type minByStream struct {
	datas Stream
	vals  Stream

	data Event
	val  float64
	done bool
}

func (self *minByStream) Next() (Event, error) {
	if self.done {
		return nil, EOI
	}

	for {
		data, err1 := self.datas.Next()
		v, err2 := self.vals.Next()
		if err1 == EOI && err2 == EOI {
			self.done = true
			return self.data, nil
		}

		if err1 != nil {
			return nil, err1
		}
		if err2 != nil {
			return nil, err2
		}

		val, ok := getIntOrFloat(v)
		if !ok {
			return nil, errors.New(fmt.Sprintf("MinBy: Expected number event, got %v", v))
		}

		if val < self.val {
			self.data = data
			self.val = val
		}
	}
}

/*
Takes a data stream and numbers stream and produces a stream with event from the data stream for which corresponding value is minimal.
*/
func MinBy(datas Stream, vals Stream) Stream {
	return &minByStream{datas, vals, nil, math.MaxFloat64, false}
}

type repeatStream struct {
	stream Stream
	filled bool
	val    Event
}

func (self *repeatStream) Next() (Event, error) {
	if !self.filled {
		v, err := self.stream.Next()
		if err != nil {
			return nil, err
		}

		self.val = v
		self.filled = true
	}
	return self.val, nil
}

/*
Create a stream of one event repeated infinitely.
*/
func Repeat(stream Stream) Stream {
	return &repeatStream{stream, false, nil}
}

type emaStream struct {
	vals    Stream
	alpha   float64
	state   float64
	started bool
}

func (self *emaStream) Next() (Event, error) {
	val, err := self.vals.Next()
	if err != nil {
		return nil, err
	}

	v, ok := getIntOrFloat(val)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Ema: Expected number event, got %v", val))
	}

	if !self.started {
		self.started = true
		self.state = v
	} else {
		self.state = self.alpha*v + (float64(1.0)-self.alpha)*self.state
	}
	return self.state, nil
}

/*
Get a numbers stream and produce a stream of EMAs of these numbers.
*/
func Ema(stream Stream, alpha float64) Stream {
	return &emaStream{stream, alpha, 0, false}
}

type rollingMaxByStream struct {
	datas Stream
	vals  Stream

	val  float64
	done bool
}

func (self *rollingMaxByStream) Next() (Event, error) {
	if self.done {
		return nil, EOI
	}

	for {
		data, err1 := self.datas.Next()
		v, err2 := self.vals.Next()
		if err1 == EOI && err2 == EOI {
			self.done = true
			return nil, EOI
		}

		if err1 != nil {
			return nil, err1
		}
		if err2 != nil {
			return nil, err2
		}

		val, ok := getIntOrFloat(v)
		if !ok {
			return nil, errors.New(fmt.Sprintf("RollingMaxBy: Expected number event, got %v", v))
		}

		if val > self.val {
			self.val = val
			return data, nil
		}
	}
}

/*
Takes a data stream and numbers stream and produces a stream with events from the data stream for which corresponding maximal value event changes.
*/
func RollingMaxBy(datas Stream, vals Stream) Stream {
	return &rollingMaxByStream{datas, vals, -math.MaxFloat64, false}
}

type rollingMinByStream struct {
	datas Stream
	vals  Stream

	val  float64
	done bool
}

func (self *rollingMinByStream) Next() (Event, error) {
	if self.done {
		return nil, EOI
	}

	for {
		data, err1 := self.datas.Next()
		v, err2 := self.vals.Next()
		if err1 == EOI && err2 == EOI {
			self.done = true
			return nil, EOI
		}

		if err1 != nil {
			return nil, err1
		}
		if err2 != nil {
			return nil, err2
		}

		val, ok := getIntOrFloat(v)
		if !ok {
			return nil, errors.New(fmt.Sprintf("RollingMinBy: Expected number event, got %v", v))
		}

		if val < self.val {
			self.val = val
			return data, nil
		}
	}
}

/*
Takes a data stream and numbers stream and produces a stream with events from the data stream for which corresponding minimal value event changes.
*/
func RollingMinBy(datas Stream, vals Stream) Stream {
	return &rollingMinByStream{datas, vals, math.MaxFloat64, false}
}

type rollingMaxByAllStream struct {
	datas Stream
	vals  Stream

	data Event
	val  float64
	done bool
}

func (self *rollingMaxByAllStream) Next() (Event, error) {
	if self.done {
		return nil, EOI
	}

	data, err1 := self.datas.Next()
	v, err2 := self.vals.Next()
	if err1 == EOI && err2 == EOI {
		self.done = true
		return nil, EOI
	}

	if err1 != nil {
		return nil, err1
	}
	if err2 != nil {
		return nil, err2
	}

	val, ok := getIntOrFloat(v)
	if !ok {
		return nil, errors.New(fmt.Sprintf("RollingMaxByAll: Expected number event, got %v", v))
	}

	if val > self.val {
		self.val = val
		self.data = data
	}

	return self.data, nil
}

/*
Takes a data stream and numbers stream and produces a stream with events from the data stream for which the current value is maximal.
*/
func RollingMaxByAll(datas Stream, vals Stream) Stream {
	return &rollingMaxByAllStream{datas, vals, nil, -math.MaxFloat64, false}
}

type rollingMinByAllStream struct {
	datas Stream
	vals  Stream

	data Event
	val  float64
	done bool
}

func (self *rollingMinByAllStream) Next() (Event, error) {
	if self.done {
		return nil, EOI
	}

	data, err1 := self.datas.Next()
	v, err2 := self.vals.Next()
	if err1 == EOI && err2 == EOI {
		self.done = true
		return nil, EOI
	}

	if err1 != nil {
		return nil, err1
	}
	if err2 != nil {
		return nil, err2
	}

	val, ok := getIntOrFloat(v)
	if !ok {
		return nil, errors.New(fmt.Sprintf("RollingMinByAll: Expected number event, got %v", v))
	}

	if val < self.val {
		self.val = val
		self.data = data
	}

	return self.data, nil
}

/*
Takes a data stream and numbers stream and produces a stream with events from the data stream for which the current value is minimal.
*/
func RollingMinByAll(datas Stream, vals Stream) Stream {
	return &rollingMinByAllStream{datas, vals, nil, math.MaxFloat64, false}
}

/*
Takes a stream af strings and append a given string to all of them.
*/
func StringAppend(stream Stream, suf string) Stream {
	return Map(stream, func(val Event) (Event, error) {
		v, ok := val.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("StringAppend: Expected event to be string, got %v", v))
		}

		return v + suf, nil
	})
}

/*
Takes a stream af strings and prepend a given string to all of them.
*/
func StringPrepend(stream Stream, pref string) Stream {
	return Map(stream, func(val Event) (Event, error) {
		v, ok := val.(string)
		if !ok {
			return nil, errors.New(fmt.Sprintf("StringPrepend: Expected event to be string, got %v", v))
		}

		return pref + v, nil
	})
}

type joinStream struct {
	streams []Stream
}

func (self *joinStream) Next() (Event, error) {
	if len(self.streams) == 0 {
		return nil, EOI
	}

	val, err := self.streams[0].Next()
	if err == EOI {
		// let GC collect the data
		self.streams[0] = nil
		self.streams = self.streams[1:]
		return self.Next()
	}
	if err != nil {
		// stop all
		self.streams = []Stream{}
		return nil, err
	}

	return val, nil
}

/*
Join multiple streams sequentially in one longer stream.
*/
func Join(streams ...Stream) Stream {
	return &joinStream{streams}
}

type drainStream interface {
	Stream
	Drain()
}

/*
Drain a stream.

This function uses a specific possibly more efficient implementation for streams that define Drain() method.
*/
func Drain(s Stream) error {
	if l, ok := s.(drainStream); ok {
		l.Drain()
		return nil
	}

	for {
		_, err := s.Next()
		if err == EOI {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

type encodeStream struct {
	base Stream
	e    Encoder
}

func (self encodeStream) Next() (Event, error) {
	evt, err := self.base.Next()
	if err != nil {
		return nil, err
	}

	return self.e.Encode(evt)
}

/*
Create a stream of encoded events of type []byte.
*/
func Encode(s Stream, e Encoder) Stream {
	return encodeStream{s, e}
}

type decodeStream struct {
	base Stream
	d    Decoder
}

func (self decodeStream) Next() (Event, error) {
	evt, err := self.base.Next()
	if err != nil {
		return nil, err
	}

	bs, ok := evt.([]byte)
	if !ok {
		return nil, errors.New(fmt.Sprintf("Decode: Expected event to be []byte, got %v", evt))
	}

	return self.d.Decode(bs)
}

/*
From a stream of []byte events create a stream of decoded events.
*/
func Decode(s Stream, d Decoder) Stream {
	return decodeStream{s, d}
}

type lenStream interface {
	Stream
	Len() int
}

/*
Get the length of a stream.

This function uses a specific possibly more efficient implementation for streams that define Len() int method.
*/
func Len(s Stream) (int, error) {
	if l, ok := s.(lenStream); ok {
		return l.Len(), nil
	}

	res := 0
	for {
		_, err := s.Next()
		if err == EOI {
			break
		}
		if err != nil {
			return res, err
		}

		res += 1
	}
	return res, nil
}
