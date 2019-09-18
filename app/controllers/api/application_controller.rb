module Api
  class ApplicationController < ActionController::Base

    include ActionView::Helpers::OutputSafetyHelper
    include ApplicationHelper
    include ActionView::Helpers::TranslationHelper
    helper_method :paginate_attrs, :strftime_time

    before_action :authenticate_user!

    class ParameterValueNotAllowed < ActionController::ParameterMissing
      attr_reader :values

      def initialize(param, values)
        @param = param
        @values = values
        super("param: #{param} value only allowed in: #{values}")
      end
    end

    class ParameterTypeNotAllowed < ActionController::ParameterMissing
      def initialize(param, type)
        super("param: #{param} type only allow: #{type}")
      end
    end

    class AccessDenied < StandardError;
    end
    class PageNotFound < StandardError;
    end

    rescue_from(ActionController::ParameterMissing) do |err|
      render json: {status: 400, message: "ParameterInvalid: #{err}"}, status: 400
    end
    rescue_from(ActiveRecord::RecordInvalid) do |err|
      render json: {status: 400, message: 'RecordInvalid'}, status: 400
    end
    rescue_from(AccessDenied) do |err|
      render json: {status: 403, message: 'AccessDenied'}, status: 403
    end
    rescue_from(ActiveRecord::RecordNotFound) do
      render json: {status: 404, message: 'Request data not found'}, status: 404
    end


    def requires!(name, opts = {})
      opts[:require] = true
      optional!(name, opts)
    end

    # r_requires! (r = require)
    # format: r_requires! :params_parent_name,[{name: mobile, values:[1,2], type:Array},{..}] => params_parent_name[:mobile] is require
    def r_requires!(params_parent_name, attrs, opts = {})
      opts[:require] = true
      optional!(params_parent_name, opts)
      if attrs.present? && attrs.is_a?(Array)
        attrs.each { |attr|
          opts[:values] = attr[:values]
          opts[:type] = attr[:type]
          opts[:parent_name] = params_parent_name
          optional!(attr[:name], opts)
        }
      else
        render_json([1, 'System Server Require Error'])
      end
    end

    # m_requires!(m = multi)
    # [:mobile,:gender],parent_name: :user => user[mobile]、user[gender] is require
    # [:mobile,:gender] => mobile、gender is require
    def m_requires!(names, opts = {})
      opts[:require] = true
      if opts[:parent_name]
        requires! :"#{opts[:parent_name]}"
      end
      if names.present? && names.is_a?(Array)
        names.each { |name|
          optional!(name, opts)
        }
      else
        render_json([1, 'System Server Require Error'])
      end
    end

    def optional!(name, opts = {})
      parent_params = opts[:parent_name]
      if parent_params.present?
        unless params[parent_params].is_a?(ActionController::Parameters)
          raise ActionController::ParameterMissing.new(parent_params)
        end
        params[name] = params[parent_params][name]
        param_name = "#{parent_params}[#{name}]"
      else
        param_name = name
      end

      if params[name].blank? && opts[:require] == true
        raise ActionController::ParameterMissing.new(param_name)
      end

      if opts[:type].present? && opts[:type] == Array
        unless params[name].is_a?(Array)
          raise ParameterTypeNotAllowed.new(param_name, opts[:type])
        end
      end

      if opts[:values] && params[name].present?
        values = opts[:values].to_a
        if !values.include?(params[name]) && !values.include?("#{params[name]}".to_i)
          raise ParameterValueNotAllowed.new(param_name, opts[:values])
        end
      end

      if params[name].blank? && opts[:default].present?
        params[name] = opts[:default]
      end
    end

    def render_json(result)
      return render json: {status: -1, message: 'render error'} unless result.is_a?(Array)
      result_length = result.length
      if result_length == 2
        json_data = {status: result[0], message: result[1]}
      else
        json_data = {status: result[0], message: result[1], data: result[2]}
      end
      render json: json_data
    end

    def first_error(obj)
      if obj
        obj.errors.full_messages[0]
      end
    end


    def current_user
      token, options = ActionController::HttpAuthentication::Token.token_and_options(request)
      # Rails.logger.warn "request headers token: #{token}"
      # Rails.logger.warn "request headers options: #{options}"
      mobile = options.blank? ? nil : options[:mobile]
      user = mobile && User.find_by(mobile: mobile)
      if user && user.access_token.present? && ActiveSupport::SecurityUtils.secure_compare(user.access_token, token)
        @current_user = user
      else
        unauthenticated!
      end
    end


    def authenticate_user!
      render_json([401, t('messages.c_401')]) unless current_user
    end

    def unauthenticated!
      render_json([401, t('messages.c_401')])
    end

    def paginate(resource)
      resource = resource.page(params[:page] || 1)
      if params[:per]
        resource = resource.per(params[:per])
      end
      resource
    end

  end
end
