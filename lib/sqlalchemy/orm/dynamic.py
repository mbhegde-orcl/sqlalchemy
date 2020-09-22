# orm/dynamic.py
# Copyright (C) 2005-2020 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

"""Dynamic collection API.

Dynamic collections act like Query() objects for read operations and support
basic add/delete mutation.

"""

from . import attributes
from . import exc as orm_exc
from . import interfaces
from . import object_mapper
from . import object_session
from . import relationships
from . import strategies
from . import util as orm_util
from .query import Query
from .. import exc
from .. import log
from .. import sql
from .. import util
from ..engine import result as _result
from ..sql import selectable
from ..sql import util as sql_util
from ..sql.base import _generative
from ..sql.base import Generative


@log.class_logger
@relationships.RelationshipProperty.strategy_for(lazy="dynamic")
class DynaLoader(strategies.AbstractRelationshipLoader):
    def init_class_attribute(self, mapper):
        self.is_class_level = True
        if not self.uselist:
            raise exc.InvalidRequestError(
                "On relationship %s, 'dynamic' loaders cannot be used with "
                "many-to-one/one-to-one relationships and/or "
                "uselist=False." % self.parent_property
            )
        elif self.parent_property.direction not in (
            interfaces.ONETOMANY,
            interfaces.MANYTOMANY,
        ):
            util.warn(
                "On relationship %s, 'dynamic' loaders cannot be used with "
                "many-to-one/one-to-one relationships and/or "
                "uselist=False.  This warning will be an exception in a "
                "future release." % self.parent_property
            )

        strategies._register_attribute(
            self.parent_property,
            mapper,
            useobject=True,
            impl_class=DynamicAttributeImpl,
            target_mapper=self.parent_property.mapper,
            order_by=self.parent_property.order_by,
            query_class=self.parent_property.query_class,
        )


class DynamicAttributeImpl(attributes.AttributeImpl):
    uses_objects = True
    default_accepts_scalar_loader = False
    supports_population = False
    collection = False
    dynamic = True

    def __init__(
        self,
        class_,
        key,
        typecallable,
        dispatch,
        target_mapper,
        order_by,
        **kw
    ):
        super(DynamicAttributeImpl, self).__init__(
            class_, key, typecallable, dispatch, **kw
        )
        self.target_mapper = target_mapper
        self.order_by = order_by
        self.query_class = AppenderQuery

    def get(self, state, dict_, passive=attributes.PASSIVE_OFF):
        if not passive & attributes.SQL_OK:
            return self._get_collection_history(
                state, attributes.PASSIVE_NO_INITIALIZE
            ).added_items
        else:
            return self.query_class(self, state)

    def get_collection(
        self,
        state,
        dict_,
        user_data=None,
        passive=attributes.PASSIVE_NO_INITIALIZE,
    ):
        if not passive & attributes.SQL_OK:
            return self._get_collection_history(state, passive).added_items
        else:
            history = self._get_collection_history(state, passive)
            return history.added_plus_unchanged

    @util.memoized_property
    def _append_token(self):
        return attributes.Event(self, attributes.OP_APPEND)

    @util.memoized_property
    def _remove_token(self):
        return attributes.Event(self, attributes.OP_REMOVE)

    def fire_append_event(
        self, state, dict_, value, initiator, collection_history=None
    ):
        if collection_history is None:
            collection_history = self._modified_event(state, dict_)

        collection_history.add_added(value)

        for fn in self.dispatch.append:
            value = fn(state, value, initiator or self._append_token)

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), state, True)

    def fire_remove_event(
        self, state, dict_, value, initiator, collection_history=None
    ):
        if collection_history is None:
            collection_history = self._modified_event(state, dict_)

        collection_history.add_removed(value)

        if self.trackparent and value is not None:
            self.sethasparent(attributes.instance_state(value), state, False)

        for fn in self.dispatch.remove:
            fn(state, value, initiator or self._remove_token)

    def _modified_event(self, state, dict_):

        if self.key not in state.committed_state:
            state.committed_state[self.key] = CollectionHistory(self, state)

        state._modified_event(dict_, self, attributes.NEVER_SET)

        # this is a hack to allow the fixtures.ComparableEntity fixture
        # to work
        dict_[self.key] = True
        return state.committed_state[self.key]

    def set(
        self,
        state,
        dict_,
        value,
        initiator=None,
        passive=attributes.PASSIVE_OFF,
        check_old=None,
        pop=False,
        _adapt=True,
    ):
        if initiator and initiator.parent_token is self.parent_token:
            return

        if pop and value is None:
            return

        iterable = value
        new_values = list(iterable)
        if state.has_identity:
            old_collection = util.IdentitySet(self.get(state, dict_))

        collection_history = self._modified_event(state, dict_)
        if not state.has_identity:
            old_collection = collection_history.added_items
        else:
            old_collection = old_collection.union(
                collection_history.added_items
            )

        idset = util.IdentitySet
        constants = old_collection.intersection(new_values)
        additions = idset(new_values).difference(constants)
        removals = old_collection.difference(constants)

        for member in new_values:
            if member in additions:
                self.fire_append_event(
                    state,
                    dict_,
                    member,
                    None,
                    collection_history=collection_history,
                )

        for member in removals:
            self.fire_remove_event(
                state,
                dict_,
                member,
                None,
                collection_history=collection_history,
            )

    def delete(self, *args, **kwargs):
        raise NotImplementedError()

    def set_committed_value(self, state, dict_, value):
        raise NotImplementedError(
            "Dynamic attributes don't support " "collection population."
        )

    def get_history(self, state, dict_, passive=attributes.PASSIVE_OFF):
        c = self._get_collection_history(state, passive)
        return c.as_history()

    def get_all_pending(
        self, state, dict_, passive=attributes.PASSIVE_NO_INITIALIZE
    ):
        c = self._get_collection_history(state, passive)
        return [(attributes.instance_state(x), x) for x in c.all_items]

    def _get_collection_history(self, state, passive=attributes.PASSIVE_OFF):
        if self.key in state.committed_state:
            c = state.committed_state[self.key]
        else:
            c = CollectionHistory(self, state)

        if state.has_identity and (passive & attributes.INIT_OK):
            return CollectionHistory(self, state, apply_to=c)
        else:
            return c

    def append(
        self, state, dict_, value, initiator, passive=attributes.PASSIVE_OFF
    ):
        if initiator is not self:
            self.fire_append_event(state, dict_, value, initiator)

    def remove(
        self, state, dict_, value, initiator, passive=attributes.PASSIVE_OFF
    ):
        if initiator is not self:
            self.fire_remove_event(state, dict_, value, initiator)

    def pop(
        self, state, dict_, value, initiator, passive=attributes.PASSIVE_OFF
    ):
        self.remove(state, dict_, value, initiator, passive=passive)


class AppenderQuery(Generative):
    """A dynamic query that supports basic collection storage operations."""

    def __init__(self, attr, state):

        # this can be select() except for aliased=True flag on join()
        # and corresponding behaviors on select().
        self._is_core = False
        self._statement = Query([attr.target_mapper], None)

        # self._is_core = True
        # self._statement = sql.select(attr.target_mapper)._set_label_style(
        #    selectable.LABEL_STYLE_TABLENAME_PLUS_COL
        # )

        self._autoflush = True
        self.instance = instance = state.obj()
        self.attr = attr

        self.mapper = mapper = object_mapper(instance)
        prop = mapper._props[self.attr.key]

        if prop.secondary is not None:
            # this is a hack right now.  The Query only knows how to
            # make subsequent joins() without a given left-hand side
            # from self._from_obj[0].  We need to ensure prop.secondary
            # is in the FROM.  So we purposely put the mapper selectable
            # in _from_obj[0] to ensure a user-defined join() later on
            # doesn't fail, and secondary is then in _from_obj[1].
            self._statement = self._statement.select_from(
                prop.mapper.selectable, prop.secondary
            )

        self._statement = self._statement.where(
            prop._with_parent(instance, alias_secondary=False),
        )

        if self.attr.order_by:

            self._statement = self._statement.order_by(*self.attr.order_by)

    @_generative
    def autoflush(self, setting):
        """Set autoflush to a specific setting.

        Note that a Session with autoflush=False will
        not autoflush, even if this flag is set to True at the
        Query level.  Therefore this flag is usually used only
        to disable autoflush for a specific Query.

        """
        self._autoflush = setting

    @property
    def statement(self):
        """Return the Core statement represented by this
        :class:`.AppenderQuery`.

        """
        if self._is_core:
            return self._statement._set_label_style(
                selectable.LABEL_STYLE_DISAMBIGUATE_ONLY
            )

        else:
            return self._statement.statement

    def filter(self, *criteria):
        """A synonym for the :meth:`_orm.AppenderQuery.where` method."""

        return self.where(*criteria)

    @_generative
    def where(self, *criteria):
        r"""Apply the given WHERE criterion, using SQL expressions.

        Equivalent to :meth:`.Select.where`.

        """
        self._statement = self._statement.where(*criteria)

    @_generative
    def order_by(self, *criteria):
        r"""Apply the given ORDER BY criterion, using SQL expressions.

        Equivalent to :meth:`.Select.order_by`.

        """
        self._statement = self._statement.order_by(*criteria)

    @_generative
    def filter_by(self, **kwargs):
        r"""Apply the given filtering criterion using keyword expressions.

        Equivalent to :meth:`.Select.filter_by`.

        """
        self._statement = self._statement.filter_by(**kwargs)

    @_generative
    def join(self, target, *props, **kwargs):
        r"""Create a SQL JOIN against this
        object's criterion.

        Equivalent to :meth:`.Select.join`.
        """

        self._statement = self._statement.join(target, *props, **kwargs)

    @_generative
    def outerjoin(self, target, *props, **kwargs):
        r"""Create a SQL LEFT OUTER JOIN against this
        object's criterion.

        Equivalent to :meth:`.Select.outerjoin`.

        """

        self._statement = self._statement.outerjoin(target, *props, **kwargs)

    def scalar(self):
        """Return the first element of the first result or None
        if no rows present.  If multiple rows are returned,
        raises MultipleResultsFound.

        Equivalent to :meth:`_query.Query.scalar`.

        .. versionadded:: 1.1.6

        """
        return self._iter().scalar()

    def first(self):
        """Return the first row.

        Equivalent to :meth:`_query.Query.first`.

        """

        # replicates limit(1) behavior
        if self._statement is not None:
            return self._iter().first()
        else:
            return self.limit(1)._iter().first()

    def one(self):
        """Return exactly one result or raise an exception.

        Equivalent to :meth:`_query.Query.one`.

        """
        return self._iter().one()

    def one_or_none(self):
        """Return one or zero results, or raise an exception for multiple
        rows.

        Equivalent to :meth:`_query.Query.one_or_none`.

        .. versionadded:: 1.0.9

        """
        return self._iter().one_or_none()

    def all(self):
        """Return all rows.

        Equivalent to :meth:`_query.Query.all`.

        """
        return self._iter().all()

    def session(self):
        sess = object_session(self.instance)
        if (
            sess is not None
            and self._autoflush
            and sess.autoflush
            and self.instance in sess
        ):
            sess.flush()
        if not orm_util.has_identity(self.instance):
            return None
        else:
            return sess

    session = property(session, lambda s, x: None)

    def _execute(self, sess=None):
        # note we're returning an entirely new Query class instance
        # here without any assignment capabilities; the class of this
        # query is determined by the session.
        instance = self.instance
        if sess is None:
            sess = object_session(instance)
            if sess is None:
                raise orm_exc.DetachedInstanceError(
                    "Parent instance %s is not bound to a Session, and no "
                    "contextual session is established; lazy load operation "
                    "of attribute '%s' cannot proceed"
                    % (orm_util.instance_str(instance), self.attr.key)
                )

        result = sess.execute(self._statement)
        result = result.scalars()

        if result._attributes.get("filtered", False):
            result = result.unique()

        return result

    def _iter(self):
        sess = self.session
        if sess is None:
            instance = self.instance
            state = attributes.instance_state(instance)

            if state.detached:
                raise orm_exc.DetachedInstanceError(
                    "Parent instance %s is not bound to a Session, and no "
                    "contextual session is established; lazy load operation "
                    "of attribute '%s' cannot proceed"
                    % (orm_util.instance_str(instance), self.attr.key)
                )
            else:
                iterator = (
                    (item,)
                    for item in self.attr._get_collection_history(
                        state, attributes.PASSIVE_NO_INITIALIZE,
                    ).added_items
                )

                row_metadata = _result.SimpleResultMetaData(
                    (self.mapper.class_.__name__,), [], _unique_filters=[id],
                )

                return _result.IteratorResult(row_metadata, iterator).scalars()
        else:
            return self._execute(sess)

    def __iter__(self):
        return iter(self._iter())

    def __getitem__(self, index):
        sess = self.session
        if sess is None:
            return self.attr._get_collection_history(
                attributes.instance_state(self.instance),
                attributes.PASSIVE_NO_INITIALIZE,
            ).indexed(index)
        else:
            return orm_util._getitem(self, index)

    @_generative
    def limit(self, limit):
        self._statement = self._statement.limit(limit)

    @_generative
    def offset(self, offset):
        self._statement = self._statement.offset(offset)

    @_generative
    def slice(self, start, stop):
        """Computes the "slice" represented by
        the given indices and apply as LIMIT/OFFSET.


        """
        limit_clause, offset_clause = sql_util._make_slice(
            self._statement._limit_clause,
            self._statement._offset_clause,
            start,
            stop,
        )

        self._statement = self._statement.limit(limit_clause).offset(
            offset_clause
        )

    def count(self):
        """return the 'count'.

        Equivalent to :meth:`_query.Query.count`.


        """

        sess = self.session
        if sess is None:
            return len(
                self.attr._get_collection_history(
                    attributes.instance_state(self.instance),
                    attributes.PASSIVE_NO_INITIALIZE,
                ).added_items
            )
        else:
            col = sql.func.count(sql.literal_column("*"))

            stmt = sql.select(col).select_from(self._statement.subquery())
            return self.session.execute(stmt).scalar()

    def extend(self, iterator):
        for item in iterator:
            self.attr.append(
                attributes.instance_state(self.instance),
                attributes.instance_dict(self.instance),
                item,
                None,
            )

    def append(self, item):
        self.attr.append(
            attributes.instance_state(self.instance),
            attributes.instance_dict(self.instance),
            item,
            None,
        )

    def remove(self, item):
        self.attr.remove(
            attributes.instance_state(self.instance),
            attributes.instance_dict(self.instance),
            item,
            None,
        )


class CollectionHistory(object):
    """Overrides AttributeHistory to receive append/remove events directly."""

    def __init__(self, attr, state, apply_to=None):
        if apply_to:
            coll = AppenderQuery(attr, state).autoflush(False)
            self.unchanged_items = util.OrderedIdentitySet(coll)
            self.added_items = apply_to.added_items
            self.deleted_items = apply_to.deleted_items
            self._reconcile_collection = True
        else:
            self.deleted_items = util.OrderedIdentitySet()
            self.added_items = util.OrderedIdentitySet()
            self.unchanged_items = util.OrderedIdentitySet()
            self._reconcile_collection = False

    @property
    def added_plus_unchanged(self):
        return list(self.added_items.union(self.unchanged_items))

    @property
    def all_items(self):
        return list(
            self.added_items.union(self.unchanged_items).union(
                self.deleted_items
            )
        )

    def as_history(self):
        if self._reconcile_collection:
            added = self.added_items.difference(self.unchanged_items)
            deleted = self.deleted_items.intersection(self.unchanged_items)
            unchanged = self.unchanged_items.difference(deleted)
        else:
            added, unchanged, deleted = (
                self.added_items,
                self.unchanged_items,
                self.deleted_items,
            )
        return attributes.History(list(added), list(unchanged), list(deleted))

    def indexed(self, index):
        return list(self.added_items)[index]

    def add_added(self, value):
        self.added_items.add(value)

    def add_removed(self, value):
        if value in self.added_items:
            self.added_items.remove(value)
        else:
            self.deleted_items.add(value)
