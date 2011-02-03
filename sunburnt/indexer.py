from copy import deepcopy
import datetime
from schema import get_attribute_or_callable
from search import LuceneQuery


class Field(object):
    def __init__(self, attribute=None, optional=False):
        self.attribute = attribute
        self.optional = optional
        self.solr_field = None


class IndexerMetaclass(type):
    """
    Metaclass that converts Field attributes to a dictionary called
    'base_fields', taking into account parent class 'base_fields' as well.
    """
    def __new__(cls, name, bases, attrs):
        super_new = super(IndexerMetaclass, cls).__new__
        parents = [b for b in bases if isinstance(b, IndexerMetaclass)]
        if not parents:
            # If this isn't a subclass of Model, don't do anything special.
            return super_new(cls, name, bases, attrs)

        base_fields = []
        meta = None
        for field_name, obj in attrs.items():
            if isinstance(obj, Field):
                base_fields.append((field_name, attrs.pop(field_name)))
            elif field_name == 'Meta':
                meta = dict((x, y) for x, y in obj.__dict__.items() if not x.startswith('__'))
                if 'documents' not in meta:
                    meta['documents'] = 100

        if meta is None:
            raise Exception('You must provide Meta attributes for the `%s` indexer.' % name)
        elif set(meta.keys()).symmetric_difference(['collection', 'documents']):
            raise Exception('Invalid Meta parameters for `%s` indexer.' % name)

        attrs['base_fields'] = dict(base_fields)
        attrs['_meta'] = meta

        new_class = super_new(cls, name, bases, attrs)
        return new_class


class BaseIndexer(object):
    def __init__(self, interface):
        self.interface = interface
        self.solr_update_timestamp = datetime.datetime.now()

        self.fields = deepcopy(self.base_fields)
        self.fields.update({
            'solr_collection': Field(),
            'solr_update_timestamp': Field(),
            })

        for field_name, field in self.fields.items():
            field.solr_field = self.interface.schema.match_field(field_name)

        self.interface.schema.check_fields(self.fields.keys())

    def transform(self, record):
        document = {}

        for field_name, field in self.fields.items():
            data = None
            try:
                if field.attribute is None:
                    if field.solr_field.dynamic:
                        if field_name.endswith('_1_TO_X'):
                            field_name = field_name.replace('_1_TO_X', '%s')
                            items = getattr(self, 'transform_%s' % (field_name % ''))(record)
                            data = [ (field_name % ('_%s' % x), y) for x, y in items.items() ]
                        else:
                            value = getattr(self, 'transform_%s' % field.solr_field.display_name(field_name))(record)
                            data = ((field_name, value),)
                    else:
                        value = getattr(self, 'transform_%s' % field_name)(record)
                        data = ((field_name, value),)
                else:
                    value = record
                    for name in field.attribute.split('.'):
                        if value is None:
                            raise AttributeError('Record: `%s` does not contain: `%s` currently trying to get: `%s`' % (record, field.attribute, name))
                        value = get_attribute_or_callable(value, name)
                    data = ((field_name, value),)
            except AttributeError:
                if not field.optional:
                    raise
            else:
                if data:
                    for name, value in data:
                        document[name] = value
        return document

    def transform_solr_collection(self, record):
        return self._meta['collection']

    def transform_solr_update_timestamp(self, record):
        return datetime.datetime.now()

    def get_records(self):
        raise NotImplementedError

    def add(self, records, commit=True):
        if not isinstance(records, (list, tuple)):
            records = [records]
        self.interface.add([ self.transform(x) for x in records ])
        if commit:
            self.interface.commit()

    def update(self):
        print "Updating documents."
        updated = 0
        additions = []
        for record in self.get_records():
            if len(additions) > self._meta['documents']:
                self.interface.add(additions)
                additions = []
            additions.append(self.transform(record))
            updated += 1

        if len(additions):
            self.interface.add(additions)
            additions = []

        self.interface.commit()
        print "Updated %s documents." % updated

        q = LuceneQuery(self.interface.schema)
        delete_query = q.Q(solr_collection=self._meta['collection']) & q.Q(solr_update_timestamp__lt=self.solr_update_timestamp)
        # We need to ensure that we are using the base Lucene Q parser and not something like the DisMax query parser.
        delete_query.local_params['lucene'] = None

        deletions = self.interface.query(delete_query).execute()
        print "Deleting %s documents" % deletions.result.numFound

        # The delete query does not accept local params and by default uses the lucene query parser
        delete_query.local_params = {}
        delete_query = unicode(delete_query)
        self.interface.delete(queries=delete_query)
        self.interface.optimize()


class Indexer(BaseIndexer):
    __metaclass__ = IndexerMetaclass
