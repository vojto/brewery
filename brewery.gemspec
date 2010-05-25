spec = Gem::Specification.new do |s| 
  s.name = "brewery"
  s.version = "0.2.0"
  s.author = "Stefan Urbanek"
  s.email = "stefan@knowerce.sk"
  s.homepage = "http://github.com/Stiivi/brewery/"
  s.platform = Gem::Platform::RUBY
  s.summary = "Brewery - data extraction, transformation, loading, analysis and mining library"
  s.files = Dir['lib/**/*.rb'] + Dir['bin/*'] + Dir['test/**/*']
  s.require_path = "lib"
  s.has_rdoc = true
  s.extra_rdoc_files = ["README"]
  s.executables << 'etl'
  s.executables << 'dataaudit'
end
