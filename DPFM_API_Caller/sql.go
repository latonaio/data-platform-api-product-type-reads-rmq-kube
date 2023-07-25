package dpfm_api_caller

import (
	"context"
	dpfm_api_input_reader "data-platform-api-product-type-reads-rmq-kube/DPFM_API_Input_Reader"
	dpfm_api_output_formatter "data-platform-api-product-type-reads-rmq-kube/DPFM_API_Output_Formatter"
	"strings"
	"sync"

	"github.com/latonaio/golang-logging-library-for-data-platform/logger"
)

func (c *DPFMAPICaller) readSqlProcess(
	ctx context.Context,
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	accepter []string,
	errs *[]error,
	log *logger.Logger,
) interface{} {
	var productType *[]dpfm_api_output_formatter.ProductType
	var productTypeText *[]dpfm_api_output_formatter.ProductTypeText
	for _, fn := range accepter {
		switch fn {
		case "ProductType":
			func() {
				productType = c.ProductType(mtx, input, output, errs, log)
			}()
		case "ProductTypes":
			func() {
				productType = c.ProductTypes(mtx, input, output, errs, log)
			}()
		case "ProductTypeText":
			func() {
				productTypeText = c.ProductTypeText(mtx, input, output, errs, log)
			}()
		case "ProductTypeTexts":
			func() {
				productTypeText = c.ProductTypeTexts(mtx, input, output, errs, log)
			}()
		default:
		}
	}

	data := &dpfm_api_output_formatter.Message{
		ProductType:     productType,
		ProductTypeText: productTypeText,
	}

	return data
}

func (c *DPFMAPICaller) ProductType(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ProductType {
	where := fmt.Sprintf("WHERE ProductType = '%s'", input.ProductType.ProductType)

	if input.ProductType.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.ProductType.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_produc_type_produc_type_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, ProductType DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductType(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ProductTypes(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ProductType {

	if input.ProductType.IsMarkedForDeletion != nil {
		where = fmt.Sprintf("%s\nAND IsMarkedForDeletion = %v", where, *input.ProductType.IsMarkedForDeletion)
	}

	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_produc_type_produc_type_data
		` + where + ` ORDER BY IsMarkedForDeletion ASC, ProductType DESC;`,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductType(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ProductTypeText(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ProductTypeText {
	var args []interface{}
	productType := input.ProductType.ProductType
	productTypeText := input.ProductType.ProductTypeText

	cnt := 0
	for _, v := range productTypeText {
		args = append(args, productType, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?,?),", cnt-1) + "(?,?)"
	rows, err := c.db.Query(
		`SELECT *
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_type_product_type_text_data
		WHERE (ProductType, Language) IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	data, err := dpfm_api_output_formatter.ConvertToProductTypeText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}

func (c *DPFMAPICaller) ProductTypeTexts(
	mtx *sync.Mutex,
	input *dpfm_api_input_reader.SDC,
	output *dpfm_api_output_formatter.SDC,
	errs *[]error,
	log *logger.Logger,
) *[]dpfm_api_output_formatter.ProductTypeText {
	var args []interface{}
	productTypeText := input.ProductType.ProductTypeText

	cnt := 0
	for _, v := range productTypeText {
		args = append(args, v.Language)
		cnt++
	}

	repeat := strings.Repeat("(?),", cnt-1) + "(?)"
	rows, err := c.db.Query(
		`SELECT * 
		FROM DataPlatformMastersAndTransactionsMysqlKube.data_platform_product_type_product_type_text_data
		WHERE Language IN ( `+repeat+` );`, args...,
	)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}
	defer rows.Close()

	//
	data, err := dpfm_api_output_formatter.ConvertToProductTypeText(rows)
	if err != nil {
		*errs = append(*errs, err)
		return nil
	}

	return data
}
