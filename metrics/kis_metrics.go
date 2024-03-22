package metrics

import (
	"kis-flow/common"
	"kis-flow/log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func RunMetricsService(serverAddr string) error {
	http.Handle(common.METRICS_ROUTE, promhttp.Handler())

	err := http.ListenAndServe(serverAddr, nil)
	if err != nil {
		log.Logger().ErrorF("RunMetricsService err = %s\n", err)
	}
	return err

}
