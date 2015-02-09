from . import JINJA_ENV


def page(request, template_name, context, _env=JINJA_ENV):
    template = _env.get_template(template_name)
    html = template.render(**context)
    if isinstance(html, unicode):
        return html.encode('utf-8')
    return html
