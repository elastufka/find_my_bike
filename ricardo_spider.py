import scrapy


class RicardoSpider(scrapy.Spider):
    name = "ricardo"

    def start_requests(self):
        urls = [
            'https://auto.ricardo.ch/de/s/moto?offer_type=classified',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_overview)

    def parse_overview(self, response):
        article_pages = response.css("a.ric-article::attr(href)")
        overview_pages = response.css("div.ric-pagination a::attr(href)")

        # go to article page
        for article_page in article_pages:
            if article_page is not None:
                yield response.follow(article_page, callback=self.parse_article)

        # go to next overview page
        for overview_page in overview_pages:
            if overview_page is not None:
                yield response.follow(overview_page, callback=self.parse_overview)

    def replace_label_value(self, label, value):
        if label == 'Hubraum':
            label = "displacement"
            value = '-' if value == '-' else float(value.replace("'", "").replace(" ccm", ""))
        if label == 'CO2 - kombiniert':
            label = "carbon_dioxide"
            print(value)
            value = '-' if value == '-' or not value or 'undefined' in value else float(value.replace("'", "").replace(" g CO2/km", ""))
        if label == 'Anzahl Gänge':
            label = "gears"
            value = '-' if value == '-' else int(value)
        if label == 'Anzahl Türen':
            label = "gears"
            value = '-' if value == '-' else int(value)
        if label == 'Zylinder':
            label = "cylinders"
            value = '-' if value == '-' else int(value)
        if label == 'Anzahl Sitze':
            label = "seats"
            value = '-' if value == '-' else int(value)
        if label == 'Leergewicht':
            label = "curb_weight"
            value = '-' if value == '-' else float(value.replace("'", "").replace(" kg", ""))
        if label == 'Ab MFK':
            label = 'has_mfk'
            value = value == 'Ja'
        if label == 'Zustand':
            label = 'condition'
        if label == 'Getriebeart':
            label = 'transmission'
        if label == 'Antriebsart':
            label = 'drive'
        if label == 'Aussenfarbe':
            label = 'colour'
        if label == 'Innenausstattung':
            label = 'interior'
        if label == 'Karosserieform':
            label = 'body_shape'
        if label == 'Kraftstoff':
            label = 'fuel'
        return {label: value}

    def parse_article(self, response):
        attributes = {
            'title': response.css("div.title h1::text").extract_first(),
            'subtitle': response.css("div.title h4.subtitle::text").extract_first(),
            'registration': response.css("div.registration div.value::text").extract_first(),
            'description': response.css("#article-description::text").extract_first(),
            'location': response.css("div.seller-info address div span::text").extract_first(),
            'image_urls': response.css("#pictures-collection img.lazy-img::attr(src)").extract(),
        }

        # extract numeric data
        performance = response.css("div.power div.value::text").extract_first()
        if None is not performance and performance is not "-":
            performance = float(performance.replace("'", "").replace(" PS", ""))
            attributes.update({"performance": performance})
        mileage = response.css("div.mileage div.value::text").extract_first()
        if None is not mileage and mileage is not "-":
            mileage = float(mileage.replace("'", "").replace(" km", ""))
            attributes.update({"mileage": mileage})
        price = response.css("div.price span:last-of-type::text").extract_first()
        if None is not price and price is not "-":
            price = float(price.replace("'", ""))
            attributes.update({"price": price})

        details = response.css(".details-list.section-list")
        for item in details.css("div.item"):
            label = item.css("span.label::text").extract_first()
            value = item.css("span.value::text").extract_first()

            attributes.update(self.replace_label_value(label, value))

        environment = response.css(".environment-details-list.section-list")
        for item in environment.css("div.item"):
            label = item.css("span.label::text").extract_first()
            value = item.css("span.value::text").extract_first()

            attributes.update(self.replace_label_value(label, value))

        yield attributes
