/*
 * Copyright 2014-2021 JetBrains s.r.o and contributors. Use of this source code is governed by the Apache 2.0 license.
 */

package io.ktor.server.html

import java.util.*

/**
 * A template that expands inside [TOuter]
 */
public interface Template<in TOuter> {
    public fun TOuter.apply()
}

/**
 * A placeholder that is inserted inside [TOuter]
 */
public open class Placeholder<TOuter> {
    private var content: TOuter.(Placeholder<TOuter>) -> Unit = { }
    public var meta: String = ""

    public operator fun invoke(meta: String = "", content: TOuter.(Placeholder<TOuter>) -> Unit) {
        this.content = content
        this.meta = meta
    }

    public fun apply(destination: TOuter) {
        destination.content(this)
    }
}

/**
 * Placeholder that can appear multiple times
 */
public open class PlaceholderList<TOuter, TInner> {
    private var items = ArrayList<PlaceholderItem<TInner>>()

    public val size: Int
        get() = items.size

    public operator fun invoke(meta: String = "", content: TInner.(Placeholder<TInner>) -> Unit = {}) {
        val placeholder = PlaceholderItem<TInner>(items.size, items)
        placeholder(meta, content)
        items.add(placeholder)
    }

    public fun isEmpty(): Boolean = items.size == 0

    public fun isNotEmpty(): Boolean = isEmpty().not()

    public fun apply(destination: TOuter, render: TOuter.(PlaceholderItem<TInner>) -> Unit) {
        for (item in items) {
            destination.render(item)
        }
    }
}

/**
 * Item of a placeholder list when it is expanded
 */
public class PlaceholderItem<TOuter>(public val index: Int, public val collection: List<PlaceholderItem<TOuter>>) :
    Placeholder<TOuter>() {
    public val first: Boolean get() = index == 0
    public val last: Boolean get() = index == collection.lastIndex
}

/**
 * Inserts every element of placeholder list
 */
public fun <TOuter, TInner> TOuter.each(
    items: PlaceholderList<TOuter, TInner>,
    itemTemplate: TOuter.(PlaceholderItem<TInner>) -> Unit
) {
    items.apply(this, itemTemplate)
}

/**
 * Inserts placeholder
 */
public fun <TOuter> TOuter.insert(placeholder: Placeholder<TOuter>): Unit = placeholder.apply(this)

/**
 * A placeholder that is also a template
 */
public open class TemplatePlaceholder<TTemplate> {
    private var content: TTemplate.() -> Unit = { }
    public operator fun invoke(content: TTemplate.() -> Unit) {
        this.content = content
    }

    public fun apply(template: TTemplate) {
        template.content()
    }
}

public fun <TTemplate : Template<TOuter>, TOuter> TOuter.insert(
    template: TTemplate,
    placeholder: TemplatePlaceholder<TTemplate>
) {
    placeholder.apply(template)
    with(template) { apply() }
}

public fun <TOuter, TTemplate : Template<TOuter>> TOuter.insert(template: TTemplate, build: TTemplate.() -> Unit) {
    template.build()
    with(template) { apply() }
}
