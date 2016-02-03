/*
 * Object Oriented Database (created by Jason MAHDJOUB (jason.mahdjoub@free.fr)) Copyright (c)
 * 2012, JBoss Inc., and individual contributors as indicated by the @authors
 * tag.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 3.0 of the License.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */



package ood.database.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation must be declared before a field contained into a class which inherits the class {@link ood.database.DatabaseRecord}. The use of this annotation means that the correspondent field is unique into the database. 
 * This annotation is not sufficient to declare a field and must be used with one of the annotations {@link ood.database.annotations.Field}, {@link ood.database.annotations.PrimaryKey}, {@link ood.database.annotations.AutoPrimaryKey}, {@link ood.database.annotations.RandomPrimaryKey}, or {@link ood.database.annotations.ForeignKey}.
 *   
 * @author Jason Mahdjoub
 * @version 1.0
 *
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Unique {
    
}