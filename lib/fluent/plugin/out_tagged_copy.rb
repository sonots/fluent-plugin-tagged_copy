require 'fluent/plugin/out_copy'

module Fluent
  class TaggedCopyOutput < CopyOutput
    Plugin.register_output('tagged_copy', self)

    def initialize
      super
      @tag_procs = []
    end 

    # Override to handle filter:tag options
    def configure(conf)
      conf.elements.select {|e|
        e.name == 'store'
      }.each {|e|
        type = e['type']
        unless type
          raise ConfigError, "Missing 'type' parameter on <store> directive"
        end 
        log.debug "adding store type=#{type.dump}"

        f = e.elements.select {|i| i.name == 'filter'}.first || {}
        @tag_procs << tag_proc(
          f['tag'],
          f['add_tag_prefix'],
          f['remove_tag_prefix'],
          f['add_tag_suffix'],
          f['remove_tag_suffix']
        )

        output = Plugin.new_output(type)
        output.configure(e)
        @outputs << output
      }
    end

    # Override to use TaggedOutputChain
    def emit(tag, es, chain)
      unless es.repeatable?
        m = MultiEventStream.new
        es.each {|time,record|
          m.add(time, record)
        }   
        es = m 
      end 
      if @deep_copy
        chain = TaggedCopyOutputChain.new(@outputs, @tag_procs, tag, es, chain)
      else
        chain = TaggedOutputChain.new(@outputs, @tag_procs, tag, es, chain)
      end 
      chain.next
    end

    private

    def tag_proc(tag, add_tag_prefix, remove_tag_prefix, add_tag_suffix, remove_tag_suffix)
      rstrip = Proc.new {|str, substr| str.chomp(substr) }
      lstrip = Proc.new {|str, substr| str.start_with?(substr) ? str[substr.size..-1] : str }
      tag_prefix = "#{rstrip.call(add_tag_prefix, '.')}." if add_tag_prefix
      tag_suffix = ".#{lstrip.call(add_tag_suffix, '.')}" if add_tag_suffix
      tag_prefix_match = "#{rstrip.call(remove_tag_prefix, '.')}." if remove_tag_prefix
      tag_suffix_match = ".#{lstrip.call(remove_tag_suffix, '.')}" if remove_tag_suffix
      tag_fixed = tag if tag
      if tag_fixed
        Proc.new {|tag| tag_fixed }
      elsif tag_prefix_match and tag_suffix_match
        Proc.new {|tag| "#{tag_prefix}#{rstrip.call(lstrip.call(tag, tag_prefix_match), tag_suffix_match)}#{tag_suffix}" }
      elsif tag_prefix_match
        Proc.new {|tag| "#{tag_prefix}#{lstrip.call(tag, tag_prefix_match)}#{tag_suffix}" }
      elsif tag_suffix_match
        Proc.new {|tag| "#{tag_prefix}#{rstrip.call(tag, tag_suffix_match)}#{tag_suffix}" }
      else
        Proc.new {|tag| "#{tag_prefix}#{tag}#{tag_suffix}" }
      end
    end
  end 

  class TaggedOutputChain
    def initialize(array, tag_procs, tag, es, chain=NullOutputChain.instance)
      @array = array
      @tag_procs = tag_procs
      @tag = tag
      @es = es
      @offset = 0
      @chain = chain
    end   
        
    def next
      if @array.length <= @offset
        return @chain.next
      end
      @offset += 1
      emit_tag = @tag_procs[@offset-1].call(@tag) # added
      result = @array[@offset-1].emit(emit_tag, @es, self)
      result
    end
  end
      
  class TaggedCopyOutputChain < TaggedOutputChain
    def next
      if @array.length <= @offset
        return @chain.next
      end
      @offset += 1
      es = @array.length > @offset ? @es.dup : @es
      emit_tag = @tag_procs[@offset-1].call(@tag) # added
      result = @array[@offset-1].emit(emit_tag, es, self)
      result
    end
  end

end


