package internal_planner

import (
	"fmt"
	"github.com/go-faster/jx"
	_ "github.com/go-faster/jx"
	"github.com/kr/logfmt"
	"golang.org/x/exp/slices"
	"regexp"
	"strconv"
)

type plainJsonParserHelper struct {
	lbls *map[string]string
}

func (p *plainJsonParserHelper) setLabels(m *map[string]string) {
	p.lbls = m
}

func (p *plainJsonParserHelper) parse(line string) error {
	dec := jx.DecodeStr(line)
	if dec.Next() != jx.Object {
		return nil
	}
	return dec.Obj(func(d *jx.Decoder, key string) error {
		return p.dec(key, d)
	})
}

func (p *plainJsonParserHelper) dec(key string, _dec *jx.Decoder) error {
	var (
		err error
		val string
	)
	switch _dec.Next() {
	case jx.String:
		val, err = _dec.Str()
	case jx.Number, jx.Bool:
		var raw jx.Raw
		raw, err = _dec.Raw()
		if err == nil {
			val = string(raw)
		}
	case jx.Object:
		err = _dec.Obj(func(d *jx.Decoder, _key string) error {
			return p.dec(key+"_"+_key, d)
		})
		val = ""
	default:
		err = _dec.Skip()
	}
	if err != nil {
		return err
	}
	if len(val) != 0 {
		(*p.lbls)[sanitizeLabel(key)] = val
	}
	return nil
}

type parameterJsonHelper struct {
	paths [][]string
	keys  []string
	lbls  *map[string]string
}

func (p *parameterJsonHelper) setLabels(m *map[string]string) {
	p.lbls = m
}

func (p *parameterJsonHelper) parse(line string) error {
	dec := jx.DecodeStr(line)
	if dec.Next() != jx.Object {
		return nil
	}
	return dec.Obj(func(d *jx.Decoder, key string) error {
		return p.dec([]string{key}, d)
	})
}

func (p *parameterJsonHelper) dec(path []string, d *jx.Decoder) error {
	idxs := p.pathIdxs(path, false)
	if len(idxs) == 0 {
		return nil
	}
	switch d.Next() {
	case jx.Object:
		return d.Obj(func(d *jx.Decoder, key string) error {
			return p.dec(append(path, key), d)
		})
	case jx.String:
		idxs = p.pathIdxs(path, true)
		val, err := d.Str()
		if err != nil {
			return err
		}
		for _, i := range idxs {
			(*p.lbls)[p.keys[i]] = val
		}
	case jx.Number, jx.Bool:
		idxs = p.pathIdxs(path, true)
		val, err := d.Raw()
		if err != nil {
			return err
		}
		for _, i := range idxs {
			(*p.lbls)[p.keys[i]] = string(val)
		}
	case jx.Array:
		i := 0
		return d.Arr(func(d *jx.Decoder) error {
			err := p.dec(append(path, strconv.FormatInt(int64(i), 10)), d)
			i++
			return err
		})
	default:
		return d.Skip()
	}
	return nil
}

func (p *parameterJsonHelper) pathIdxs(path []string, fullMatch bool) []int {
	var res []int
	for i := range p.paths {
		if len(path) > len(p.paths[i]) {
			continue
		}
		match := true
		for j := range path {
			if p.paths[i][j] != path[j] {
				match = false
				break
			}
		}
		if match && (!fullMatch || len(p.paths[i]) == len(path)) {
			res = append(res, i)
		}
	}
	return res
}

func (p *ParserPlanner) json(str string, labels *map[string]string) (map[string]string, error) {
	dec := jx.DecodeStr(str)
	if dec.Next() != jx.Object {
		return nil, fmt.Errorf("not an object")
	}
	err := p.subDec(dec, "", labels)
	return *labels, err
}

func (p *ParserPlanner) subDec(dec *jx.Decoder, prefix string, labels *map[string]string) error {
	return dec.Obj(func(d *jx.Decoder, key string) error {
		_prefix := prefix
		if _prefix != "" {
			_prefix += "_"
		}
		_prefix += key
		switch d.Next() {
		case jx.Object:
			return p.subDec(d, _prefix, labels)
		case jx.String:
			val, err := d.Str()
			if err != nil {
				return err
			}
			(*labels)[sanitizeLabel(_prefix)] = val
			return nil
		case jx.Array:
			return d.Skip()
		default:
			raw, err := d.Raw()
			if err != nil {
				return err
			}
			(*labels)[sanitizeLabel(_prefix)] = raw.String()
			return nil
		}
	})
}

type plainLogfmtHelper struct {
	lbls *map[string]string
}

func (l *plainLogfmtHelper) setLabels(m *map[string]string) {
	l.lbls = m
}

func (l *plainLogfmtHelper) HandleLogfmt(key, val []byte) error {
	(*l.lbls)[sanitizeLabel(string(key))] = string(val)
	return nil
}

func (l *plainLogfmtHelper) parse(line string) error {
	err := logfmt.Unmarshal([]byte(line), l)
	return err
}

type parameterLogfmtHelper struct {
	lbls  *map[string]string
	keys  []string
	paths []string
}

func (p *parameterLogfmtHelper) setLabels(m *map[string]string) {
	p.lbls = m
}

func (p *parameterLogfmtHelper) HandleLogfmt(key, val []byte) error {
	_key := string(key)
	idx := slices.Index(p.paths, _key)
	if idx != -1 {
		(*p.lbls)[sanitizeLabel(_key)] = string(val)
	}
	return nil
}

func (p *parameterLogfmtHelper) parse(line string) error {
	err := logfmt.Unmarshal([]byte(line), p)
	return err
}

var sanitizeRe = regexp.MustCompile("[^a-zA-Z0-9_]")

func sanitizeLabel(label string) string {
	return sanitizeRe.ReplaceAllString(label, "_")
}
