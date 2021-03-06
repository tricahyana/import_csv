# import_csv
Faster &amp; most memory efficient to import large csv file in Ruby.

Library ini bertujuan untuk optimasi proses import data dari file CSV yang berukuran besar atau lebih dari 10M. Jika menggunakan cara konvensional, seluruh file CSV akan di load kedalam memory lalu akan di parsing sehingga setiap baris nya merupakan array. Untuk ukuran file CSV yang tidak terlalu besar, kurang dari 1M, cara seperti ini mungkin akan lebih cepat dan lebih mudah, namun jika ukuran file CSV lebih dari 1M maka akan menjadi masalah karena akan memakan memory yang sangat banyak dan proses bukannya semakin cepat malah akan semakin lambat, terlebih yang paling berbahaya adalah kemungkinan server crash. Adapun cara lain yang lebih hemat memory adalah dengan membaca satu-persatu line dalam file tersebut lalu memparsing nya kedalam array, cara ini akan jauh lebih hemat memory karena tidak seluruh isi file dimasukan kedalam memory. Namun kelemhan dari proses ini adalah setiap row nya harus di parsing ke dalam array satu persatu dan akan cukup memakan waktu.

Library ini bekerja dengan mengambil line satu-persatu lalu disimpan kedalam sebuah variable, tidak sampai semua isi file disimpan kedalam variable tersebut namun sampai jumlah preload terpenuhi -default nya 1000 line. Jika jumlah preload tersebut telah terpenuhi, maka variable tersebut akan diparsing kedalam bentuk array yang mana hasil parsing-an tersebut akan digunakan untuk keperluan pengolahan data. Cara seperti ini akan hemat memory namun juga lebih cepat karena proses parsing akan dilakukan sekaligus. Untuk lebih detail teknis nya sendiri dapat dilihat di dalam file impor_csv/import.rb.

Contoh :
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
   csv.each do |line|
     p line.current[0]
     p line.current[1]
     ... your code ...
   end

   ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv')) do |line|
     p line.current[0]
     p line.current[1]
     ... your code ...
   end

   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true) do
     p csv['location_id']
     p csv['location_name']
     ... your code ...
   end

   - Setting preload data dengan mengisi atribute atau di dalam parameter contructor
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
   csv.preload = 2000
   csv.each do |line|
     p line.current[0]
     p line.current[1]
     ... your code ...
   end

   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), preload: 2000)
   csv.each do |line|
     p line.current[0]
     p line.current[1]
     ... your code ...
   end

   - Setting Automatic header, membuat header berdasarkan row pertama dalam file. Selanjutnya row pertama akan diabaikan.
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true, preload: 2000)
   csv.each do
     p csv.loation_id
     p csv.location_name
     ... your code ...
   end

   - Mendefinisikan sendiri header, jika ini dilakukan maka row pertama akan di proses
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: ['location_id', 'location_name'])
   csv.each do
     p csv.location_id
     p csv.location_name
     ... your code ...
   end

   - Cara lain mendefinisikan header.
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
   csv.set_header ['location_id', 'location_name']
   csv.each do
     p csv.location_id
     p csv.location_name
     ... your code ...
   end

   - Memanggil data seperti memanggil hash. Perlu diperhatikan bahwa object `csv` bukan object Hash.
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
   csv.set_header ['location_id', 'location_name']
   csv.each do
     p csv['location_id']
     p csv['location_name']
     ... your code ...
   end

   - Setting parse option. (Read: http://ruby-doc.org/stdlib-2.0.0/libdoc/csv/rdoc/CSV.htmlclass-CSV-label-CSV+and+Character+Encodings+-28M17n+or+Multilingualization-29)
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true, parse_options: {col_sep: ';', quote_char: '"'})
   csv.each do |line|
     p line['location_id']
     p line['location_name']
     ... your code ...
   end

   - Pararel processing. Tidak seperti contoh-contoh diatas, jika `return_preload_only` di set `true` maka akan `each` akan mengembalikan array dari preload data (seperti yang dihasilkan oleh CSV.parse('some text'))
   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true, return_preload_only: true)
   csv.each do |preload_data|
     CsvWorker.perform_async(preload_data)
   end
