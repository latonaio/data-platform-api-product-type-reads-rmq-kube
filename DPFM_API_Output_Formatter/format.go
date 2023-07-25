package dpfm_api_output_formatter

import (
	"data-platform-api-product-type-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToProductType(rows *sql.Rows) (*[]ProductType, error) {
	defer rows.Close()
	productType := make([]ProductType, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.ProductType{}

		err := rows.Scan(
			&pm.ProductType,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &productType, nil
		}

		data := pm
		productType = append(productType, ProductType{
			ProductType:			data.ProductType,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &productType, nil
}

func ConvertToProductTypeText(rows *sql.Rows) (*[]ProductTypeText, error) {
	defer rows.Close()
	productTypeText := make([]ProductTypeText, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.ProductTypeText{}

		err := rows.Scan(
			&pm.ProductType,
			&pm.Language,
			&pm.ProductTypeName,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &productTypeText, err
		}

		data := pm
		productTypeText = append(productTypeText, ProductTypeText{
			ProductType:     		data.ProductType,
			Language:          		data.Language,
			ProductTypeName:		data.ProductTypeName,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &productTypeText, nil
}
