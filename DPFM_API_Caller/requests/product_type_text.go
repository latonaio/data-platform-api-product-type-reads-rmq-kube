package requests

type ProductTypeText struct {
	ProductType     	string  `json:"ProductType"`
	Language          	string  `json:"Language"`
	ProductTypeName		string  `json:"ProductTypeName"`
	CreationDate		string	`json:"CreationDate"`
	LastChangeDate		string	`json:"LastChangeDate"`
	IsMarkedForDeletion	*bool	`json:"IsMarkedForDeletion"`
}
