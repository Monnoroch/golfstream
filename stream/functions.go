package stream

import (
	"errors"
	"fmt"
)

// Register stream functions pre-defined by this library.
func RegisterDefault() {
	RegisterDefaultEncoders()
	RegisterDefaultDecoders()

	Register("", id)
	Register("id", id)
	Register("load", load)
	Register("save", save)
	Register("encode", encode)
	Register("decode", decode)
	Register("zip", zip)
	Register("get_field", getField)
	Register("set_field", setField)
	Register("==", eq)
	Register("!=", neq)
	Register(">", more)
	Register(">=", moreEq)
	Register("<", less)
	Register("<=", lessEq)
	Register("&&", and)
	Register("||", or)
	Register("filter", filter)
	Register("max_by", maxBy)
	Register("min_by", minBy)
	Register("repeat", repeat)
	Register("ema", ema)
	Register("ema_n", emaN)
	Register("max_by_roll", rollingMaxBy)
	Register("min_by_roll", rollingMinBy)
	Register("max_by_roll_all", rollingMaxByAll)
	Register("min_by_roll_all", rollingMinByAll)
	Register("append", sappend)
	Register("prepend", sprepend)
}

func id(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 1 {
		return nil, errors.New(fmt.Sprintf("id: Expected 1 args, got %v", len(args)))
	}

	return build(ctx, args[0])
}

func load(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 1 && len(args) != 2 && len(args) != 3 {
		return nil, errors.New(fmt.Sprintf("load: Expected 1 or 2 or 3 args, got %v", len(args)))
	}

	name, ok := args[0].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("load: Expected args[0] to be string, got %v", args[0]))
	}

	sctx, ok := ctx[name]
	if !ok {
		return nil, errors.New(fmt.Sprintf("load: There is no Stream with name %s: %v", name, ctx))
	}

	return sctx.mp.New(), nil
}

func save(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("save: Expected 2 args, got %v", len(args)))
	}

	name, ok := args[0].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("save: Expected args[0] to be string, got %v", args[0]))
	}

	_, ok = ctx[name]
	if ok {
		return nil, errors.New(fmt.Sprintf("save: There already is a Stream with the name %s: %v", name, ctx))
	}

	proc, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	ctx[name] = &StreamContext{
		stream: proc,
		mp:     Multiplexer(proc),
	}
	return Empty(), nil
}

func zip(ctx Context, args []FArg) (Stream, error) {
	if len(args) <= 1 {
		return nil, errors.New(fmt.Sprintf("zip: Expected more than 1 arg, got %v", len(args)))
	}

	streams := make([]Stream, len(args))
	for i, a := range args {
		s, err := build(ctx, a)
		if err != nil {
			return nil, err
		}

		streams[i] = s
	}

	return Zip(streams...), nil
}

func getField(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("get_field: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	field, ok := args[1].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("get_field: Expected args[1] to be string, got %v", args[1]))
	}

	return GetField(proc, field), nil
}

func setField(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 3 {
		return nil, errors.New(fmt.Sprintf("set_field: Expected 3 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	field, ok := args[1].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("set_field: Expected args[1] to be string, got %v", args[1]))
	}

	vals, err := build(ctx, args[2])
	if err != nil {
		return nil, err
	}

	return SetField(datas, vals, field), nil
}

func eq(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("==: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	return EqVal(proc, args[1]), nil
}

func neq(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("!=: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	return NeqVal(proc, args[1]), nil
}

func more(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf(">: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := getIntOrFloat(args[1])
	if !ok {
		return nil, errors.New(fmt.Sprintf(">: Expected args[1] to be number, got %v", args[1]))
	}

	return MoreVal(proc, arg), nil
}

func moreEq(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf(">=: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := getIntOrFloat(args[1])
	if !ok {
		return nil, errors.New(fmt.Sprintf(">=: Expected args[1] to be number, got %v", args[1]))
	}

	return MoreEqVal(proc, arg), nil
}

func less(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("<: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := getIntOrFloat(args[1])
	if !ok {
		return nil, errors.New(fmt.Sprintf("<: Expected args[1] to be number, got %v", args[1]))
	}

	return LessVal(proc, arg), nil
}

func lessEq(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("<=: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := getIntOrFloat(args[1])
	if !ok {
		return nil, errors.New(fmt.Sprintf("<=: Expected args[1] to be number, got %v", args[1]))
	}

	return LessEqVal(proc, arg), nil
}

func or(ctx Context, args []FArg) (Stream, error) {
	if len(args) <= 1 {
		return nil, errors.New(fmt.Sprintf("||: Expected > 1 args, got %v", len(args)))
	}

	streams := make([]Stream, len(args))
	for i, v := range args {
		s, err := build(ctx, v)
		if err != nil {
			return nil, err
		}

		streams[i] = s
	}

	return Or(streams...), nil
}

func and(ctx Context, args []FArg) (Stream, error) {
	if len(args) <= 1 {
		return nil, errors.New(fmt.Sprintf("&&: Expected > 1 args, got %v", len(args)))
	}

	streams := make([]Stream, len(args))
	for i, v := range args {
		s, err := build(ctx, v)
		if err != nil {
			return nil, err
		}

		streams[i] = s
	}

	return And(streams...), nil
}

func filter(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("filter: Expected 2 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	flags, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	return Filter(datas, flags), nil
}

func maxBy(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("max_by: Expected 2 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	vals, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	return MaxBy(datas, vals), nil
}

func minBy(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("min_by: Expected 2 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	vals, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	return MinBy(datas, vals), nil
}

func repeat(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 1 {
		return nil, errors.New(fmt.Sprintf("repeat: Expected 1 arg, got %v", len(args)))
	}

	vals, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	return Repeat(vals), nil
}

func ema(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("ema: Expected 2 args, got %v", len(args)))
	}

	vals, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	alpha, ok := getIntOrFloat(args[1])
	if !ok {
		return nil, errors.New(fmt.Sprintf("ema: Expected number as args[1], got %v", args[1]))
	}

	return Ema(vals, alpha), nil
}

func emaN(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("ema_n: Expected 2 args, got %v", len(args)))
	}

	vals, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	n, ok := getIntOrFloat(args[1])
	if !ok {
		return nil, errors.New(fmt.Sprintf("ema_n: Expected number as args[1], got %v", args[1]))
	}

	return Ema(vals, float64(1.0)/(n+float64(1.0))), nil
}

func rollingMaxBy(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("max_by_roll: Expected 2 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	vals, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	return RollingMaxBy(datas, vals), nil
}

func rollingMinBy(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("min_by_roll: Expected 2 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	vals, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	return RollingMinBy(datas, vals), nil
}

func rollingMaxByAll(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("max_by_roll_all: Expected 2 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	vals, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	return RollingMaxByAll(datas, vals), nil
}

func rollingMinByAll(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("min_by_roll_all: Expected 2 args, got %v", len(args)))
	}

	datas, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	vals, err := build(ctx, args[1])
	if err != nil {
		return nil, err
	}

	return RollingMinByAll(datas, vals), nil
}

func sappend(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("append: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := args[1].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("append: Expected args[1] to be string, got %v", args[1]))
	}

	return StringAppend(proc, arg), nil
}

func sprepend(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("prepend: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := args[1].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("prepend: Expected args[1] to be string, got %v", args[1]))
	}

	return StringPrepend(proc, arg), nil
}

func encode(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("encode: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := args[1].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("encode: Expected args[1] to be string, got %v", args[1]))
	}

	e, ok := getEncoder(arg)
	if !ok {
		return nil, errors.New(fmt.Sprintf("encode: No encoder with name \"%s\"", arg))
	}

	return Encode(proc, e), nil
}

func decode(ctx Context, args []FArg) (Stream, error) {
	if len(args) != 2 {
		return nil, errors.New(fmt.Sprintf("decode: Expected 2 args, got %v", len(args)))
	}

	proc, err := build(ctx, args[0])
	if err != nil {
		return nil, err
	}

	arg, ok := args[1].(string)
	if !ok {
		return nil, errors.New(fmt.Sprintf("decode: Expected args[1] to be string, got %v", args[1]))
	}

	e, ok := getDecoder(arg)
	if !ok {
		return nil, errors.New(fmt.Sprintf("decode: No encoder with name \"%s\"", arg))
	}

	return Decode(proc, e), nil
}
