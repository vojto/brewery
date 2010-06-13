class Hash
def hash_by_symbolising_keys
    hash = self.class.new
    
    self.keys.each { |key|
        hash[key.to_sym] = self[key]
    }
    
    return hash
    
end

end
