require 'csv'

# Kasyfil Aziz Tri Cahyana <tricahyana@windowslive.com> <kasyfil.aziz@wgs.co.id> 2016
#
# require all file below in yours Ruby on Rails Application
# lib/import_csv/import.rb
#
# This library can make import data from large csv (>10M) faster and very low
# memory usage (depend on preload data setting).
#
# This library will get data line by line and parse to array, it's using CSV.parse
# but more efficient. Rather than parse line by line, this library will load some
# line to memory first and parse to array. It called preload data. Default
# preload data is 1000, but you can define preload data by your own. You can also
# configure parse option like CSV parse option in standart ruby library.
#
# Example :
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
#   while csv.next
#     p csv.current[0]
#     p csv.current[1]
#     ... your code ...
#   end
#
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
#   csv.each do |line|
#     p line[0]
#     p line[1]
#     ... your code ...
#   end
#
#   ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv')) do |line|
#     p line[0]
#     p line[1]
#     ... your code ...
#   end
#
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true) do |line|
#     p line['location_id']
#     p line['location_name']
#     ... your code ...
#   end
#
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true)
#   while csv.next
#     p csv.location_id
#     p csv.location_name
#     ... your code ...
#   end
#
#   - Setting preload data. You can setting preload data by set preload attribute
#     or in ImportCSV.new
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
#   csv.preload = 2000
#   csv.each do |line|
#     p line[0]
#     p line[1]
#     ... your code ...
#   end
#
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), preload: 2000)
#   csv.each do |line|
#     p line[0]
#     p line[1]
#     ... your code ...
#   end
#
#   - Setting Automatic header, get header from first row in file and ignore
#     first row.
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true, preload: 2000)
#   csv.each do
#     p csv.loation_id
#     p csv.location_name
#     ... your code ...
#   end
#
#   - Define header by your self. If you define header your self, this script
#     will not ignore first row in file.
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: ['location_id', 'location_name'])
#   csv.each do
#     p csv.location_id
#     p csv.location_name
#     ... your code ...
#   end
#
#   - Define header by instance method.
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
#   csv.set_header ['location_id', 'location_name']
#   csv.each do
#     p csv.location_id
#     p csv.location_name
#     ... your code ...
#   end
#
#   - Call data using hash. Header must be set.
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
#   csv.set_header ['location_id', 'location_name']
#   csv.each do
#     p csv['location_id']
#     p csv['location_name']
#     ... your code ...
#   end
#
#   - Setting parse option. (Read: http://ruby-doc.org/stdlib-2.0.0/libdoc/csv/rdoc/CSV.html#class-CSV-label-CSV+and+Character+Encodings+-28M17n+or+Multilingualization-29)
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true, parse_options: {col_sep: ';', quote_char: '"'})
#   csv.each do |line|
#     p line['location_id']
#     p line['location_name']
#     ... your code ...
#   end
#
#   - Pararel processing. Instead loop through preload data, each method can pass
#     array of preload data so you can send to background process like Sidekiq.
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true, return_preload_only: true)
#   csv.each_preload do |preload_data|
#     CsvWorker.perform_async(preload_data)
#   end
#
#   - Filter data. More example, see documenttation in `where` mothode below.
#   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true)
#   csv.where(location_name: 'jakarta')
#   csv.each do |line|
#     p line.location_id
#     p line.location_name
#     ... your code ...
#   end
#
class ImportCSV
  # preload data
  attr_accessor :preload

  # current line number in file
  attr_accessor :line_count

  # file path (string)
  attr_accessor :file_path

  # file object (File)
  attr_accessor :file_object

  # header (Array)
  attr_accessor :header

  # header (Boolean)
  attr_accessor :has_header
  attr_accessor :define_header_by_your_self

  # current line in csv file if header has been define
  attr_accessor :current

  # current preload data
  attr_accessor :current_preload

  # Boolean. If true, will loop through file and send current preload data to
  # block function
  attr_accessor :return_preload_only

  # set parse options
  attr_accessor :parse_options

  attr_accessor :query

  attr_accessor :background_task

  attr_accessor :next_preload

  attr_accessor :file_eof

  # Class constructor.
  # set file path and preload data
  # if block given, then will call `each` so you can add block line using `each`
  #
  # Example :
  #   ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv')) do |line|
  #     p line.current[0]
  #     p line.current[1]
  #     ... your code ...
  #   end
  #
  def initialize(file_path, options = Hash.new)
    self.preload = options[:preload] || 1000
    self.line_count = 1
    self.file_path = file_path
    self.parse_options = options[:parse_options] || {}
    self.return_preload_only = options[:return_preload_only] || false
    self.file_object = File.open(self.file_path, 'r')
    self.background_task = nil
    self.next_preload = []
    self.file_eof = false
    if options[:header]
      self.has_header = true
      if options[:header].kind_of?(Array)
        self.header = options[:header]
      end
      # create attribute
      self.header_generator
    end
    self.current = []
    self.current_preload = []
    self.query = Hash.new
    if block_given?
      self.each { |line, line_count| yield line, line_count }
    else
      self
    end
  end

  # return file name
  def file_name
    File.basename self.file_object
  end

  # return preload data, not a single line but preload data. Size of preload
  # data is depend on preload attribute, default is 1000
  #
  # this method can be usefull for parallel processing
  #
  def each_preload(return_hash = self.has_header)
    if self.query.empty?
      if return_hash
        while self.perform_preload
          yield create_hash
        end
      else
        while self.perform_preload
          yield self.current_preload
        end
      end
    else
      if return_hash
        while self.perform_filter
          yield create_hash
        end
      else
        while self.perform_filter
          yield self.current_preload
        end
      end
    end
  end

  def create_hash
    result_hash = []
    self.current_preload.each do |preload|
      result_hash << Hash[self.header.zip(preload)]
    end
    return result_hash
  end

  # set header, so you can call atribute based on header.
  # Example:
  #   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
  #   csv.set_header ['location_id', 'location_date', 'departure_date']
  #   csv.each do
  #     p csv.location_id
  #     p csv.location_date
  #     ... your code ...
  #   end
  #
  # Parameter must be an array, if not, will raise an ArgumentError
  #
  def set_header(header)
    if header.kind_of?(Array)
      # set header & has_header
      self.header = header.map(&:downcase)
      self.has_header = true
      self.define_header_by_your_self = true
      self.header_generator
    else
      raise ArgumentError, "header must be an array"
    end
  end

  def get_header_index(header)
    return header if header.kind_of?(Integer)
    return self.header.index(header.to_s.downcase)
  end

  alias :define_header :set_header

  # create atribute based on header.
  # you don't have to call this method in your code.
  def header_generator
    if self.has_header
      if self.header.kind_of?(Array)
        header_line = self.header
      else
        self.header = CSV.parse(self.file_object.readline).first.map(&:downcase)
        header_line = self.header
      end
      # create atribute based on csv header
      header_line.each_with_index do |header, index|
        self.define_singleton_method(header.downcase.gsub(/[^A-Za-z]/, '_')) do
          return self.current[index]
        end
      end
      self.define_singleton_method(:[]) do |key|
        return self.current[header_line.index(key.downcase)]
      end
      return self.header
    else
      return false
    end
  end

  # Loop through csv file.
  #
  # Example :
  #   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'))
  #   csv.each do |line, line_count|
  #     p line[0]
  #     p line[1]
  #     ... your code ...
  #   end
  #
  # Example :
  #   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true)
  #   csv.each do |line, line_count|
  #     csv.location_id
  #     csv.location_name
  #     ... your code ...
  #   end
  #
  def each
    if self.has_header
      while self.next
        yield self, self.line_count
      end
    else
      while self.next
        yield self.current, self.line_count
      end
    end
  end

  # Get next line from CSV. This method actualy return data from preload variable,
  # if preload empty this method will call `perform_preload` or `filter` -if
  # query is not empty- to fill data to preload variable.
  #
  # After call this method, data will store on object (in `current` variable or
  # in method with same name with header).
  #
  # Example :
  #   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true)
  #   csv.next
  #   p csv.location_id ~> `return first line from file`
  #   csv.next
  #   p csv.location_id ~> `return second line from file`
  #
  # Example using while :
  #   csv = ImportCSV.new(Rails.root.join('db/seeds/development/tx_locations.csv'), header: true)
  #   while csv.next
  #     p csv.location_id
  #   end
  #
  def next
    # if current_preload is empty, this method will call `perform_preload` or
    # `filter` to fill current_preload with the data.
    if self.current_preload.empty?
      # if cursor reach end of file, then will return false. This is usefull if
      # you call this method in `while`. See example above.
      if self.file_object.eof?
        self.current = []
        return false
      else
        # determine which preload method will call, perform_preload which is get
        # data without any filter.
        #
        # to perfom `filter preload` you must set filter in `where` method. See
        # example in that method.
        if self.query.empty?
          self.perform_preload
        else
          self.perform_filter
        end
      end
    end
    # if preload method above return empty data, return false.
    if !self.current_preload.empty?
      # set current parsed line from first element in `current` attribute
      self.current = self.current_preload.first
      # delete first element in current_preload atribute
      self.current_preload.shift
      self.line_count += 1
      return true
    else
      return false
    end
  end

  # Get n line from csv file and parse. n is `preload` attribute. Default value
  # for preload is 1000, you can change this value in this class constructor.
  # See `initialize` method for more example.
  #
  # This method will return false if cursor has been reach end of line in csv
  # file. Otherwise, return true.
  #
  def perform_preload
    _preload
    # if self.background_task.nil?
    #   if _preload
    #     _background_preload
    #     return true
    #   else
    #     return false
    #   end
    # else
    #   return false if self.file_eof
    #   ThreadsWait.join(self.background_task)
    #   self.current_preload = self.next_preload
    #   _background_preload
    #   return true
    # end
  end

  ##
  # Experimental
  # Currently not working
  #
  # run preload on background
  def _background_preload
    mutex = Mutex.new
    self.background_task = Thread.fork do
      mutex.synchronize do
        # for temporary data before parse to array
        _row = String.new
        for i in 1.upto(self.preload)
          # if self.file_object.eof?
            # parse last data
            # break
          # else
          begin
            # add line in file to temporary data
            _row += self.file_object.readline
          rescue EOFError => e
            self.file_eof = true
            break
          end
          # end
        end

        # parse data
        begin
          self.next_preload = CSV.parse(_row, self.parse_options)
        rescue => e
          debugger
          raise e
        end
      end
      Thread.current.exit
    end
  end

  def _preload
    return false if self.file_object.eof?

    # for temporary data before parse to array
    row = ''
    for i in 1.upto(self.preload)
      if self.file_object.eof?
        # parse last data
        # self.current_preload = CSV.parse(row, self.parse_options)
        break
      else
        # add line in file to temporary data
        row << self.file_object.readline
      end
    end
    # parse data
    self.current_preload = CSV.parse(row, self.parse_options)
    return true
  end

  # Set filter. You can use this operator ['>', '<', '!', '%'] and Range to
  # perform filter.
  #
  # Before set filter, you must set header true or define header by yourself.
  # For set header true and define header, see example above.
  #
  # Example:
  #   CSV data:
  #    __________________________
  #   |id |  name    | birth     |
  #   |1  |  shania  | 27-06-1998|
  #   |2  |  jessica | 19-08-1993|
  #   |3  |  michelle| 28-10-1999|
  #   |___|__________|___________|
  #
  #   Equal.
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(name: 'michelle')
  #       ... use csv.each or while csv.next ...
  #       ~> will return [3, 'michelle', '28-10-1999']
  #
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(name: ['shania', 'jessica'])
  #       ... use csv.each or while csv.next ...
  #       ~> will return [[1, 'shania', '27-06-1998'], [2, 'jessica', '19-08-1993']]
  #
  #   Range. Only for Date, Integer and Float data type. Define datatype in
  #   first range. Use `integer` for Integer & Float, use `date` for Date.
  #   See Example below.
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(id: 1..2)
  #       ... use csv.each or while csv.next ...
  #       => will return [[1, 'shania', '27-06-1998'], [2, 'jessica', '19-08-1993']]
  #
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(birth: '01-01-1993'.to_date..'01-01-1999'.to_date)
  #       ... use csv.each or while csv.next ...
  #       => will return [[1, 'shania', '27-06-1998'], [2, 'jessica', '19-08-1993']]
  #
  #   Operator '>' & '<'. Only for column with data type Integer, Float or Date
  #   Like `id` or `birth` in example csv data above.
  #     - Data type must defined in filter, use `integer` for Integer or Float
  #       and use `date` for Date. Put operator & data type together without
  #       space. See example below.

  #     - For filter with Date data type (in csv or in filter), any value that
  #       can be parse using `Date.parse` are acceptable.
  #
  #         csv = ImportCSV.new('member.csv'), header: true)
  #         csv.where(id: '>(integer)1')
  #         ... use csv.each or while csv.next ...
  #         => will return [[2, 'jessica', '19-08-1993'], [3, 'michelle', '28-10-1999']]
  #
  #         csv = ImportCSV.new('member.csv'), header: true)
  #         csv.where(birth: '<(date)01-01-1997')
  #         ... use csv.each or while csv.next ...
  #         => will return [2, 'jessica', '19-08-1993']
  #
  #   Operator '!'. Put this operator in first character and folow with query
  #   without space.
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(name: '!michelle')
  #       ... use csv.each or while csv.next ...
  #       => will return [[1, 'shania', '27-06-1998'], [2, 'jessica', '19-08-1993']]
  #
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(name: ['!shania', '!jessica'])
  #       ... use csv.each or while csv.next ...
  #       => will return [3, 'michelle', '28-10-1999']
  #
  #   Operator '%'. `Like` Operator. Put this operator in first character and
  #   folow with query without space.
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(name: '%jes')
  #       ... use csv.each or while csv.next ...
  #       => will return [2, 'jessica', '19-08-1993']
  #
  #     csv = ImportCSV.new('member.csv'), header: true)
  #     csv.where(name: ['%jes', '%shan'])
  #       ... use csv.each or while csv.next ...
  #       => will return [[1, 'shania', '27-06-1998'], [2, 'jessica', '19-08-1993']]
  #
  # Note :
  #  - Data type must define if you use `<` or `>`.
  #
  def where(query = Hash.new)
    # if !self.has_header
    #   raise ArgumentError, 'Header not detected.'
    # end

    query.each do |key, values|
      if values.kind_of?(Range)
        if values.first.kind_of?(String) || values.last.kind_of?(String)
          raise ArgumentError, "Range filter only accept Date, Time, Integer or Float data type."
        end

        if values.first > values.last
          raise ArgumentError, "First value is larger than last value."
        end
      end
    end

    self.query = self.query.merge(query)
    # for chaining
    self
  end

  def clear_filter
    self.query = Hash.new
    self
  end

  # Perform preload with filter data. Call `where` with query first before call
  # this method.
  #
  def perform_filter(query = self.query)
    # list of operator that can be used.
    filter_operation = ['>', '<', '!', '%']
    col_sep = self.parse_options[:col_sep] || ','
    row_sep = self.parse_options[:row_sep] || "\n"
    # temporary preload data.
    row = []
    # first loop to make sure temporary preload size is same as defined preload
    # size
    loop do
      row_tmp = []
      # preload data
      for i in 1.upto(self.preload)
        if self.file_object.eof?
          # break the loop if reach end of line
          break
        else
          # get line and split to array elament by column separator,
          _row_tmp = self.file_object.readline.split(col_sep)
          is_insert = false
          # loop throug query difined in `where`
          query.each do |key, values|
            # if value is Range, (integer)1..2 or (date)01-01-2015..01-01-2016
            if values.kind_of?(Range)
              # scan for data type insert brackets.
              if values.first.kind_of?(Date) || values.first.kind_of?(Time)
                # tmp_range_first = Date.parse(values.first[6..values.first.size])
                tmp_range_first = values.first
                tmp_range_last = values.last
                # remove quote \" and `new line` from string
                tmp_value = Date.parse(_row_tmp[self.get_header_index(key)].gsub(/\A"|"\Z/, '').gsub(row_sep, ''))
              elsif values.first.kind_of?(Integer) || values.first.kind_of?(Float)
                # value with type integer will convert to float
                # tmp_range_first = (values.first[9..values.first.size]).to_f
                tmp_range_first = (values.first).to_f
                tmp_range_last = (values.last).to_f
                tmp_value = (_row_tmp[self.get_header_index(key)].gsub(/\A"|"\Z/, '').gsub(row_sep, '')).to_f
              end

              # comparation
              if (tmp_value >= tmp_range_first) && (tmp_value <= tmp_range_last)
                is_insert = true
                break
              else
                is_insert = false
              end
            else
              # if value from query is not array, then will be conver to array
              # with only one element
              if !values.kind_of?(Array)
                values = [values]
              end

              #loop throug value from query
              values.each do |value|
                # check operator from first caracter in value, is any operator
                # define or not. if not, then will goto `equal`
                if filter_operation.include?(value[0])
                  if value[0] === '>' || value[0] === '<'
                    # scan for data type insert brackets.
                    if value.scan(/\(([^\)]+)\)/)[0][0].downcase == "date"
                      tmp_filter = Date.parse(value[7..value.size])
                      # remove quote \" and `new line` from string
                      tmp_value = Date.parse(_row_tmp[self.get_header_index(key)].gsub(/\A"|"\Z/, '').gsub(row_sep, ''))
                    elsif value.scan(/\(([^\)]+)\)/)[0][0].downcase == "integer"
                      tmp_filter = value[10..value.size].to_f
                      tmp_value = _row_tmp[self.get_header_index(key)].gsub(/\A"|"\Z/, '').gsub(row_sep, '').to_f
                    end

                    # comparation
                    if value[0] === '>'
                      if tmp_value > tmp_filter
                        is_insert = true
                        break
                      else
                        is_insert = false
                      end
                    elsif value[0] === '<'
                      if tmp_value < tmp_filter
                        is_insert = true
                        break
                      else
                        is_insert = false
                      end
                    end

                  elsif value[0] === '!'
                    # remove quote \" and `new line` from string
                    if _row_tmp[self.get_header_index(key)].gsub(/\A"|"\Z/, '').gsub(row_sep, '') != value[1..value.size]
                      is_insert = true
                    else
                      is_insert = false
                      break
                    end
                  elsif value[0] === '%'
                    # remove quote \" and `new line` from string
                    if _row_tmp[self.get_header_index(key)].gsub(/\A"|"\Z/, '').gsub(row_sep, '').include?(value[1..value.size])
                      is_insert = true
                      break
                    else
                      is_insert = false
                    end

                  else
                    # raise an ArgumentError (Exception) if opertor is not one
                    # of which has been defined
                    raise ArgumentError, 'Operator not allowed. Use one of this [>, <, !, %].'
                  end
                else
                  # remove quote \" and `new line` from string
                  if _row_tmp[self.get_header_index(key)].gsub(/\A"|"\Z/, '').gsub(row_sep, '') === (value)
                    is_insert = true
                    break
                  else
                    is_insert = false
                  end
                end
              end
            end
            # go to next line if query return false
            break if !is_insert
          end
          # insert to temporary accepted row if all query return true
          row_tmp << _row_tmp.join(col_sep) if is_insert
        end
      end
      row.push(*row_tmp) if row_tmp.size > 0
      break if row.size >= self.preload || self.file_object.eof?
    end
    self.current_preload = CSV.parse(row.join(col_sep).gsub("#{row_sep},", row_sep), self.parse_options)
  end

  # if object from this class will reuse, call this method to reopen file so you
  # can read file again.
  def reopen
    begin
      self.file_object = File.open(self.file_path, 'r')
      self.line_count = 1
      self.current = []
      self.current_preload = []
      self.file_object.readline if self.has_header && !self.define_header_by_your_self
      true
    rescue => e
      raise e
    end
  end

  def self.export(file)
    if block_given?
      File.open(file, 'w')
      File.open(file, 'a') do |file|
        yield file
      end
    else
      raise NotImplementedError, "block must given."
    end
  end
end
