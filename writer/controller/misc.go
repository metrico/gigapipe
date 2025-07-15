package controller

var Ready = Build(withOkStatusAndBody(200, []byte("ok")))

var Config = Build(withOkStatusAndBody(200, []byte("Not supported")))

var HealthInflux = Build(withOkStatusAndBody(200, nil))

var PromHealthStub = Build(withOkStatusAndBody(200, nil))
