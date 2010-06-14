class Numeric
def to_string_with_delimiter(delimiter = ' ', separator = '.')
    begin
        parts = self.to_s.split('.')
        parts[0].gsub!(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1#{delimiter}")
        parts.join(separator)
    rescue
        self
    end
end
def to_string_with_precision(precision, delimiter = ' ', separator = '.')
    rounded_number = (Float(self) * (10 ** precision)).round.to_f / 10 ** precision
    begin
        str = "%01.#{precision}f" % rounded_number
        parts = str.to_s.split('.')
        parts[0].gsub!(/(\d)(?=(\d\d\d)+(?!\d))/, "\\1#{delimiter}")
        parts.join(separator)
    rescue
        self
    end
end

end
