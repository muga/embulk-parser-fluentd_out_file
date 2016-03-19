
Gem::Specification.new do |spec|
  spec.name          = "embulk-parser-fluentd_out_file"
  spec.version       = "0.1.0"
  spec.authors       = ["Muga Nishizawa"]
  spec.summary       = %[Fluentd Out File parser plugin for Embulk]
  spec.description   = %[Parses Fluentd Out File files read by other file input plugins.]
  spec.email         = ["muga.nishizawa@gmail.com"]
  spec.licenses      = ["Apache 2.0"]
  spec.homepage      = "https://github.com/muga/embulk-parser-fluentd_out_file"

  spec.files         = `git ls-files`.split("\n") + Dir["classpath/*.jar"]
  spec.test_files    = spec.files.grep(%r"^(test|spec)/")
  spec.require_paths = ["lib"]

  spec.add_development_dependency 'bundler', ['~> 1.0']
  spec.add_development_dependency 'rake', ['>= 10.0']
end
