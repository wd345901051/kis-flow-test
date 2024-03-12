package file

import (
	"errors"
	"fmt"
	"io/ioutil"
	"kis-flow/common"
	"kis-flow/kis"

	"gopkg.in/yaml.v3"
)

func ConfigExportYaml(flow kis.Flow, savePath string) error {
	if data, err := yaml.Marshal(flow.GetConfig()); err != nil {
		return err
	} else {
		err := ioutil.WriteFile(fmt.Sprintf("%s%s-%s.yaml", savePath, common.KisIdTypeFlow, flow.GetName()), data, 0644)
		if err != nil {
			return err
		}

		for _, fp := range flow.GetConfig().Flows {
			fConf := flow.GetFuncConfigByName(fp.FuncName)
			if fConf == nil {
				return errors.New(fmt.Sprintf("function name = %s config is nil ", fp.FuncName))
			}

			if fdata, err := yaml.Marshal(fConf); err != nil {
				return err
			} else {
				if err := ioutil.WriteFile(fmt.Sprintf("%s%s-%s.yaml", savePath, common.KisIdTypeFunction, fp.FuncName), fdata, 0644); err != nil {
					return err
				}
			}

			if fConf.Option.CName != "" {
				cConf, err := fConf.GetConnConfig()
				if err != nil {
					return err
				}
				if cdata, err := yaml.Marshal(cConf); err != nil {
					return err
				} else {
					if err := ioutil.WriteFile(fmt.Sprintf("%s%s-%s.yaml", savePath, common.KisIdTypeConnnector, cConf.CName), cdata, 0644); err != nil {
						return err
					}
				}
			}
		}
	}
	return nil
}
