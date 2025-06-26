# Copyright 2023 Axini B.V. https://www.axini.com, see: LICENSE.txt.
# frozen_string_literal: true

# Abstract base for SUT handlers.
class Handler
  # Handler config (set by AdapterCore).
  attr_accessor :configuration

  # Registers AdapterCore for callbacks.
  def register_adapter_core(adapter_core)
    @adapter_core = adapter_core
    @configuration = default_configuration
  end

  # Start SUT connection.
  def start
    raise NoMethodError, ABSTRACT_METHOD
  end

  # Stop SUT connection.
  def stop
    raise NoMethodError, ABSTRACT_METHOD
  end

  # Reset SUT for next test.
  def reset
    raise NoMethodError, ABSTRACT_METHOD
  end

  # Stimulate SUT with label.
  # @param [PluginAdapter::Api:Label] stimulus to inject into the SUT.
  def stimulate(_label)
    raise NoMethodError, ABSTRACT_METHOD
  end

  # Returns supported labels.
  # @return [<PluginAdapter::Api:Label>] The labels supported by the plugin adapter.
  def supported_labels
    raise NoMethodError, ABSTRACT_METHOD
  end

  # Returns default config.
  # The default configuration for this plugin adapter.
  def default_configuration
    raise NoMethodError, ABSTRACT_METHOD
  end

  ABSTRACT_METHOD = 'abstract method: should be implemented by subclass'
  private_constant :ABSTRACT_METHOD
end
