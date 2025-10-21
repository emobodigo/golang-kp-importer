build:
	go build -o dist/import_tool .

outlet:
	./dist/import_tool outlet --file ./uploads/outlet.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

product:
	./dist/import_tool product --file ./uploads/product.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

stock:
	./dist/import_tool stock --file ./uploads/stock.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

invoice-return:
	./dist/import_tool invoice-return --file ./uploads/invoice-return.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

invoice-return-product:
	./dist/import_tool invoice-return-product --file ./uploads/invoice-return-product.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --batch 500

invoice:
	./dist/import_tool invoice --file ./uploads/invoice.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

invoice-product:
	./dist/import_tool invoice-product --file ./uploads/invoice-product.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 

invoice-fee:
	./dist/import_tool invoice-fee --file ./uploads/invoice-fee.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --batch 500

invoice-outstanding:
	./dist/import_tool invoice-outstanding --file ./uploads/invoice-outstanding.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

invoice-outstanding-product:
	./dist/import_tool invoice-outstanding-product --file ./uploads/invoice-outstanding-product.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

deposit:
	./dist/import_tool deposit --file ./uploads/deposit.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

giro:
	./dist/import_tool giro --file ./uploads/giro.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

settlement:
	./dist/import_tool settlement --file ./uploads/settlement.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 

intransit:
	./dist/import_tool intransit --file ./uploads/intransit.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

intransit-product:
	./dist/import_tool intransit-product --file ./uploads/intransit-product.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true"  --batch 500

invoice-missing:
	./dist/import_tool invoice --file ./uploads/invoice-missing.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

invoice-product-missing:
	./dist/import_tool invoice-product-missing --file ./uploads/invoice-product-missing.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

transfer:
	./dist/import_tool transfer --file ./uploads/transfer.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 

balance:
	./dist/import_tool balance --file ./uploads/balance.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 --batch 500

dmf:
	./dist/import_tool dmf --file ./uploads/dmf.xlsx --dsn "root:@tcp(127.0.0.1:3306)/web_kebayoran_new?parseTime=true&multiStatements=true" --admin-id 1 

.PHONY: build outlet product stock invoice invoice-product invoice-fee invoice-return invoice-return-product invoice-outstanding deposit giro settlement intransit intransit-product transfer balance dmf