package main

import (
	"fmt"
	"os"

	"github.com/emobodigo/golang-kp-importer/src"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: import_tool [outlet|product|stock] [options]")
		os.Exit(1)
	}

	cmd := os.Args[1]

	switch cmd {
	case "outlet":
		src.RunImportOutletCmd(os.Args[2:])
	case "product":
		src.RunImportProductCmd(os.Args[2:])
	case "stock":
		src.RunImportInitialStockCmd(os.Args[2:])
	case "invoice":
		src.RunImportSalesInvoiceCmd(os.Args[2:])
	case "invoice-product":
		src.RunImportSalesInvoiceProductCmd(os.Args[2:])
	case "invoice-fee":
		src.RunImportSalesInvoiceFeeCmd(os.Args[2:])
	case "invoice-return":
		src.RunImportSalesInvoiceReturnCmd(os.Args[2:])
	case "invoice-return-product":
		src.RunImportSalesInvoiceReturnProductCmd(os.Args[2:])
	case "invoice-outstanding":
		src.RunImportSalesInvoiceOutstandingCmd(os.Args[2:])
	case "invoice-outstanding-product":
		src.RunImportSalesInvoiceProductOutstandingCmd(os.Args[2:])
	case "deposit":
		src.RunImportDepositCmd(os.Args[2:])
	case "giro":
		src.RunImportGiroCmd(os.Args[2:])
	case "settlement":
		src.RunImportSettlementCmd(os.Args[2:])
	case "intransit":
		src.RunImportSKBCentralIntransitCmd(os.Args[2:])
	case "intransit-product":
		src.RunImportSKBCentralIntransitProductCmd(os.Args[2:])
	case "invoice-product-missing":
		src.RunImportSalesInvoiceProductMissingCmd(os.Args[2:])
	case "transfer":
		src.RunImportTransferOutstandingCmd(os.Args[2:])
	case "balance":
		src.RunImportBeginningBalanceCmd(os.Args[2:])
	case "dmf":
		src.RunImportDMFCmd(os.Args[2:])
	default:
		fmt.Printf("Unknown command: %s\n", cmd)
		fmt.Println("Usage: import_tool [outlet|product|stock] [options]")
		os.Exit(1)
	}
}
